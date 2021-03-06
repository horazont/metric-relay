use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use log::trace;

use chrono::{DateTime, Duration, DurationRound, Utc};

use crate::metric;
use crate::metric::{MaskedArray, MaskedArrayWriter};
use crate::serial::SerialNumber;

#[derive(Debug, Clone, Copy)]
pub enum WriteError {
	/// The timestamp of the block is too far in the past to be accepted.
	///
	/// This is a permanent error.
	InThePast,

	/// The timestamp of the block is too far in the future.
	///
	/// The write may succeed of other writes happen in the meantime and/or
	/// after an unspecified delay.
	InTheFuture,

	/// The period is unacceptable for the write buffer.
	///
	/// This is a permanent error.
	InvalidPeriod,

	/// The number of samples in the block is too large.
	TooLong,
}

impl fmt::Display for WriteError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::InThePast => f.write_str("t0 is too far in the past"),
			Self::InTheFuture => f.write_str("t0 is too far in the future"),
			Self::InvalidPeriod => f.write_str("block period is invalid for this buffer"),
			Self::TooLong => f.write_str("too many samples in block"),
		}
	}
}

impl std::error::Error for WriteError {}

pub trait StreamBuffer {
	/// Write a block of data into the buffer.
	fn write(&mut self, block: &metric::StreamBlock) -> Result<(), WriteError>;

	/// Return the next available block from the buffer.
	///
	/// Only completed blocks are returned. A single call to
	/// [`StreamBuffer::write`] may make zero or more blocks available for
	/// reading.
	#[must_use = "the data is removed from the buffer"]
	fn read_next(&mut self) -> Option<metric::StreamBlock>;

	fn slice(&self) -> Duration;

	fn valid_period(&self, period: std::time::Duration) -> bool {
		let slice = self.slice().num_nanoseconds().unwrap();
		let period = match Duration::from_std(period) {
			Ok(v) => match v.num_nanoseconds() {
				Some(v) => v,
				None => return false,
			},
			Err(_) => return false,
		};
		let nsamples = match slice.checked_div(period) {
			Some(v) => v,
			None => return false,
		};
		nsamples * period == slice
	}
}

#[derive(Debug, Clone)]
enum Writer {
	I16(MaskedArrayWriter<i16>),
	F64(MaskedArrayWriter<f64>),
}

impl Writer {
	fn into_inner(self) -> metric::RawData {
		match self {
			Self::I16(data) => metric::RawData::I16(data.into_inner()),
			Self::F64(data) => metric::RawData::F64(data.into_inner()),
		}
	}

	fn from_subblock(other: &metric::RawData, start_at: usize, block_size: usize) -> Self {
		match other {
			metric::RawData::I16(data) => {
				let mut buf: MaskedArrayWriter<_> =
					MaskedArray::masked_with_value(block_size, 0).into();
				buf.copy_from_slice(&data[start_at..]);
				Self::I16(buf)
			}
			metric::RawData::F64(data) => {
				let mut buf: MaskedArrayWriter<_> =
					MaskedArray::masked_with_value(block_size, 0.).into();
				buf.copy_from_slice(&data[start_at..]);
				Self::F64(buf)
			}
		}
	}

	fn empty_with_type(block_size: usize, typeref: &metric::RawData) -> Self {
		match typeref {
			metric::RawData::I16(_) => {
				let buf: MaskedArrayWriter<_> =
					MaskedArray::masked_with_value(block_size, 0).into();
				Self::I16(buf)
			}
			metric::RawData::F64(_) => {
				let buf: MaskedArrayWriter<_> =
					MaskedArray::masked_with_value(block_size, 0.).into();
				Self::F64(buf)
			}
		}
	}

	fn capacity(&self) -> usize {
		match self {
			Self::I16(data) => data.capacity(),
			Self::F64(data) => data.capacity(),
		}
	}

	fn cursor(&self) -> usize {
		match self {
			Self::I16(data) => data.cursor(),
			Self::F64(data) => data.cursor(),
		}
	}

	fn setpos(&mut self, at: usize) {
		match self {
			Self::I16(data) => data.setpos(at),
			Self::F64(data) => data.setpos(at),
		}
	}

	fn copy_from_block(&mut self, block: &metric::RawData, up_to: usize) -> usize {
		match self {
			Self::I16(data) => match block {
				metric::RawData::I16(ref v) => {
					data.copy_from_slice(&v[..up_to]);
					v.len().saturating_sub(up_to)
				}
				_ => panic!("mismatching data types"),
			},
			Self::F64(data) => match block {
				metric::RawData::F64(ref v) => {
					data.copy_from_slice(&v[..up_to]);
					v.len().saturating_sub(up_to)
				}
				_ => panic!("mismatching data types"),
			},
		}
	}
}

impl From<MaskedArray<i16>> for Writer {
	fn from(other: MaskedArray<i16>) -> Self {
		Self::I16(other.into())
	}
}

impl From<MaskedArray<f64>> for Writer {
	fn from(other: MaskedArray<f64>) -> Self {
		Self::F64(other.into())
	}
}

#[derive(Debug, Clone)]
struct BufferedBlock {
	t0: DateTime<Utc>,
	seq0: SerialNumber,
	path: metric::DevicePath,
	period: Duration,
	scale: metric::Value,
	data: Writer,
}

impl From<BufferedBlock> for metric::StreamBlock {
	fn from(other: BufferedBlock) -> Self {
		Self {
			t0: other.t0,
			seq0: other.seq0.into(),
			path: other.path,
			period: other.period.to_std().unwrap(),
			scale: other.scale,
			data: Arc::new(other.data.into_inner()),
		}
	}
}

#[derive(Debug, Clone)]
pub struct InMemoryBuffer {
	slice: Duration,
	emit_blocks: VecDeque<metric::StreamBlock>,
	next_block: Option<BufferedBlock>,
	// reference timestamp and sequence number if available. this is stored when a block is created and it points to the beginning of the earliest block where we can collect data
	// this allows to match based on the sequence number if the timestamps are not off too far, which means that we can avoid gaps if the clock is drifting slightly
	reference: Option<(DateTime<Utc>, SerialNumber)>,
}

impl InMemoryBuffer {
	pub fn new(slice: Duration) -> Self {
		Self {
			reference: None,
			next_block: None,
			emit_blocks: VecDeque::new(),
			slice,
		}
	}

	fn update_reference(&mut self) {
		if let Some(next_block) = self.next_block.as_ref() {
			self.reference = Some((next_block.t0, next_block.seq0));
		}
	}

	/// Match a given block t0 and seq0 against the available reference.
	///
	/// If no reference is available or if it is too far off, the timestamp
	/// and sequence number of the incoming blocks metadata will be used.
	fn match_reference(
		&mut self,
		t0: DateTime<Utc>,
		seq0: SerialNumber,
		period: Duration,
	) -> (DateTime<Utc>, DateTime<Utc>, SerialNumber) {
		let in_block_t0 = t0.duration_trunc(period).unwrap();
		let out_block_t0 = in_block_t0.duration_trunc(self.slice).unwrap();
		let time_based_out_block_seq0 = seq0
			- (((in_block_t0 - out_block_t0).num_nanoseconds().unwrap()
				/ period.num_nanoseconds().unwrap()) as u16);

		match self.reference {
			None => {
				trace!(
					"using inbound block {} @ {} as reference; mapped to outbound block {} @ {}",
					t0,
					seq0,
					out_block_t0,
					time_based_out_block_seq0
				);
				(in_block_t0, out_block_t0, time_based_out_block_seq0)
			}
			Some((ref_t0, ref_seq0)) => {
				// NOTE: this assumes that output blocks are smaller than 20k samples, because counter wraparound happens at 60ksamples and we need some headroom
				let dt = t0 - ref_t0;
				if dt > period * 32765 {
					// too far in the future, we have to resync
					trace!("matching inbound block {} @ {} to outbound block {} @ {} by timestamp because timestamp is too far in the future", t0, seq0, out_block_t0, time_based_out_block_seq0);
					(in_block_t0, out_block_t0, time_based_out_block_seq0)
				} else {
					// we can meaningfully look at the sequence number, because streams never move backwards
					// if the sequence number difference is small enough, we use our stored t0 to calculate a new t0
					match seq0.partial_cmp(&ref_seq0) {
						Some(Ordering::Equal) | Some(Ordering::Greater) => {
							// sequence number is moving forward
							let dseq = seq0 - ref_seq0;
							assert!(dseq > 0);
							let dseq = dseq as u16;
							let samples_per_block = (self.slice.num_nanoseconds().unwrap()
								/ period.num_nanoseconds().unwrap()) as u16;
							let in_block_t0 = ref_t0 + period * dseq as i32;
							let out_block_t0 = in_block_t0.duration_trunc(self.slice).unwrap();
							let out_block_seq0 =
								ref_seq0 + (dseq / samples_per_block) * samples_per_block;
							trace!("matching inbound block {} @ {} to outbound block {} @ {} by sequence number (dseq={}, ref_seq0={}, ref_t0={})", t0, seq0, out_block_t0, out_block_seq0, dseq, ref_seq0, ref_t0);
							(in_block_t0, out_block_t0, out_block_seq0.into())
						}
						Some(Ordering::Less) | None => {
							// sequence number is moving backward............. let's give time based matching a shot
							trace!("matching inbound block {} @ {} to outbound block {} @ {} by timestamp because sequence number is in the past/too far in the future", t0, seq0, out_block_t0, time_based_out_block_seq0);
							(in_block_t0, out_block_t0, time_based_out_block_seq0)
						}
					}
				}
			}
		}
	}
}

impl StreamBuffer for InMemoryBuffer {
	fn write(&mut self, block: &metric::StreamBlock) -> Result<(), WriteError> {
		if block.data.len() >= 32768 {
			return Err(WriteError::TooLong);
		}

		let period = Duration::from_std(block.period).unwrap();
		if period > self.slice {
			return Err(WriteError::InvalidPeriod);
		}

		let samples_per_block =
			self.slice.num_nanoseconds().unwrap() / period.num_nanoseconds().unwrap();
		let samples_per_block = if samples_per_block <= 0 || samples_per_block >= 20000 {
			return Err(WriteError::InvalidPeriod);
		} else {
			samples_per_block as usize
		};

		let in_block_seq0: SerialNumber = block.seq0.into();
		let (_, out_block_t0, out_block_seq0) =
			self.match_reference(block.t0, in_block_seq0, period);

		let next_block = match self.next_block.as_mut() {
			None => {
				// TODO: use Option::insert once available
				trace!(
					"starting new block at t0 = {} [no existing block]",
					out_block_t0
				);
				self.next_block = Some(BufferedBlock {
					period,
					t0: out_block_t0,
					seq0: out_block_seq0,
					path: block.path.clone(),
					scale: block.scale.clone(),
					data: Writer::empty_with_type(samples_per_block, &*block.data),
				});
				self.update_reference();
				self.next_block.as_mut().unwrap()
			}
			Some(v) => {
				if period == v.period
					&& block.scale == v.scale
					&& block.path == v.path
					&& v.seq0 <= in_block_seq0
					&& in_block_seq0 < v.seq0 + (v.data.capacity() as u16)
				{
					v
				} else {
					// we have to flush the current block to allow a safe overwrite of self.next_block
					drop(v);
					let next_block = self.next_block.take().unwrap();
					// TODO: smarter interpolation
					self.emit_blocks.push_back(next_block.into());

					trace!(
						"starting new block at t0 = {} [mismatching parameters]",
						out_block_t0
					);
					self.next_block = Some(BufferedBlock {
						period,
						t0: out_block_t0,
						seq0: out_block_seq0,
						path: block.path.clone(),
						scale: block.scale.clone(),
						data: Writer::empty_with_type(samples_per_block, &*block.data),
					});
					self.update_reference();
					self.next_block.as_mut().unwrap()
				}
			}
		};

		let relative_in_seq0 = (in_block_seq0 - next_block.seq0) as usize;
		let relative_in_seq1 = relative_in_seq0 + block.data.len();
		assert!(relative_in_seq0 < next_block.data.capacity());
		if relative_in_seq0 < next_block.data.cursor() {
			// we have this data already, drop it
			return Err(WriteError::InThePast);
		}
		next_block.data.setpos(relative_in_seq0);

		let max_take = if relative_in_seq1 < next_block.data.capacity() {
			relative_in_seq1 - relative_in_seq0
		} else {
			next_block.data.capacity() - relative_in_seq0
		};

		assert_eq!(
			max_take != (relative_in_seq1 - relative_in_seq0),
			max_take < block.data.len()
		);

		let remaining = next_block.data.copy_from_block(&*block.data, max_take);

		if next_block.data.cursor() == next_block.data.capacity() {
			// emit the block
			drop(next_block);
			self.emit_blocks
				.push_back(self.next_block.take().unwrap().into());
		} else {
			drop(next_block);
		}

		// now we only need to handle the overhang
		if remaining > 0 {
			let new_t0 = out_block_t0 + self.slice;
			trace!("starting new block at t0 = {}  [overhang]", new_t0);
			self.next_block = Some(BufferedBlock {
				period,
				// NOTE: we are deliberately ignoring the t0 of the inbound block here
				// the idea is that we do not have to rely on potentially incorrect clocks in such cases
				// in general, you'll note that most of the matching happens based on seq0, as long as the block broadly fits into the currently active slice.
				t0: new_t0,
				seq0: out_block_seq0 + (samples_per_block as u16),
				path: block.path.clone(),
				scale: block.scale.clone(),
				data: Writer::from_subblock(&*block.data, max_take, samples_per_block),
			});
			self.update_reference();
		}

		Ok(())
	}

	fn read_next(&mut self) -> Option<metric::StreamBlock> {
		self.emit_blocks.pop_front()
	}

	fn slice(&self) -> Duration {
		self.slice
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use chrono::{DurationRound, TimeZone};

	fn new_buffer() -> InMemoryBuffer {
		InMemoryBuffer::new(Duration::minutes(1))
	}

	fn epoch() -> DateTime<Utc> {
		Utc.ymd(2021, 8, 5).and_hms(7, 30, 12)
	}

	fn period() -> Duration {
		Duration::milliseconds(100)
	}

	fn data_block(seq0: u16, nsamples: u16) -> metric::StreamBlock {
		let period = period();
		let t0 = epoch().duration_trunc(period).unwrap() + period * seq0 as i32;
		let mut data = Vec::with_capacity(nsamples as usize);
		data.resize(nsamples as usize, 2342i16);
		metric::StreamBlock {
			t0,
			path: metric::DevicePath {
				instance: "0".into(),
				device_type: "test_device".into(),
			},
			seq0,
			period: period.to_std().unwrap(),
			scale: metric::Value {
				magnitude: 1.0,
				unit: metric::Unit::Arbitrary,
			},
			data: Arc::new(metric::RawData::I16(data.into())),
		}
	}

	fn jittered_block(seq0: u16, nsamples: u16, t_offset: Duration) -> metric::StreamBlock {
		let mut block = data_block(seq0, nsamples);
		block.t0 = block.t0 + t_offset;
		block
	}

	#[test]
	fn selftest() {
		assert_eq!(0u16, 65536u32 as u16);
	}

	#[test]
	fn writes_to_two_minutes_cause_one_emit() {
		let b1 = data_block(0, 10);
		let b2 = data_block(600, 10);
		let mut buf = new_buffer();
		match buf.write(&b1) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.read_next() {
			None => (),
			other => panic!("unexpected read result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.write(&b2) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.read_next() {
			Some(v) => {
				assert_eq!(v.seq0, 65535 - 119);
				assert_eq!(v.t0, Utc.ymd(2021, 8, 5).and_hms(7, 30, 0));
				assert_eq!(v.data.len(), 600);
				match *v.data {
					metric::RawData::I16(ref v) => {
						for (i, v) in v[..120].iter().enumerate() {
							if *v != 0 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 0)
							}
						}
						for (i, v) in v[120..130].iter().enumerate() {
							if *v != 2342 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 2342)
							}
						}
						for (i, v) in v[130..].iter().enumerate() {
							if *v != 0 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 0)
							}
						}
					}
					#[allow(unreachable_patterns)]
					ref other => panic!("unexpected raw data: {:?}", other),
				}
			}
			other => panic!("unexpected read result: {:?}", other),
		};
	}

	#[test]
	fn matches_on_seq0_if_available() {
		let b1 = data_block(0, 10);
		let b2 = jittered_block(10, 10, Duration::milliseconds(200));
		let b3 = data_block(600, 10);
		let mut buf = new_buffer();
		match buf.write(&b1) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		match buf.write(&b2) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		match buf.write(&b3) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.read_next() {
			Some(v) => {
				assert_eq!(v.seq0, 65535 - 119);
				assert_eq!(v.t0, Utc.ymd(2021, 8, 5).and_hms(7, 30, 0));
				assert_eq!(v.data.len(), 600);
				match *v.data {
					metric::RawData::I16(ref v) => {
						for (i, v) in v[..120].iter().enumerate() {
							if *v != 0 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 0)
							}
						}
						for (i, v) in v[120..140].iter().enumerate() {
							if *v != 2342 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 2342)
							}
						}
						for (i, v) in v[140..].iter().enumerate() {
							if *v != 0 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 0)
							}
						}
					}
					#[allow(unreachable_patterns)]
					ref other => panic!("unexpected raw data: {:?}", other),
				}
			}
			other => panic!("unexpected read result: {:?}", other),
		};
	}

	#[test]
	fn straddling_write_causes_emit() {
		let b1 = data_block(479, 2);
		let b2 = data_block(1079, 2);
		let mut buf = new_buffer();
		match buf.write(&b1) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.read_next() {
			Some(v) => {
				assert_eq!(v.seq0, 65535 - 119);
				assert_eq!(v.t0, Utc.ymd(2021, 8, 5).and_hms(7, 30, 0));
				assert_eq!(v.data.len(), 600);
				match *v.data {
					metric::RawData::I16(ref v) => {
						for (i, v) in v[..599].iter().enumerate() {
							if *v != 0 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 0)
							}
						}
						for (i, v) in v[599..].iter().enumerate() {
							if *v != 2342 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 2342)
							}
						}
					}
					#[allow(unreachable_patterns)]
					ref other => panic!("unexpected raw data: {:?}", other),
				}
			}
			other => panic!("unexpected read result: {:?}", other),
		};
		match buf.write(&b2) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.read_next() {
			Some(v) => {
				assert_eq!(v.seq0, 480);
				assert_eq!(v.t0, Utc.ymd(2021, 8, 5).and_hms(7, 31, 0));
				assert_eq!(v.data.len(), 600);
				match *v.data {
					metric::RawData::I16(ref v) => {
						for (i, v) in v[..1].iter().enumerate() {
							if *v != 2342 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 2342)
							}
						}
						for (i, v) in v[1..599].iter().enumerate() {
							if *v != 0 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 0)
							}
						}
						for (i, v) in v[599..].iter().enumerate() {
							if *v != 2342 {
								panic!("sample {} is incorrect: {:?} != {}", i, v, 2342)
							}
						}
					}
					#[allow(unreachable_patterns)]
					ref other => panic!("unexpected raw data: {:?}", other),
				}
			}
			other => panic!("unexpected read result: {:?}", other),
		};
		assert!(buf.read_next().is_none());
	}

	#[test]
	fn matches_on_seq0_correctly_for_multiple_blocks() {
		let b1 = data_block(0, 10);
		let b2 = data_block(470, 20);
		let b3 = data_block(600, 10);
		let mut buf = new_buffer();
		match buf.write(&b1) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.write(&b2) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.write(&b3) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		assert!(buf.read_next().is_some());
		match buf.read_next() {
			None => (),
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn matches_on_seq0_correctly_on_multi_block_gap() {
		let b1 = data_block(0, 10);
		let b2 = data_block(470, 20);
		let b3 = data_block(2400, 10);
		let mut buf = new_buffer();
		match buf.write(&b1) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.write(&b2) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		match buf.write(&b3) {
			Ok(_) => (),
			other => panic!("unexpected write result: {:?}", other),
		};
		println!("{:?}", buf);
		assert!(buf.read_next().is_some());
		match buf.read_next() {
			Some(_) => (),
			other => panic!("unexpected read result: {:?}", other),
		}
		match buf.read_next() {
			None => (),
			other => panic!("unexpected read result: {:?}", other),
		}
	}
}
