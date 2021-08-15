use std::convert::TryFrom;
use std::io;
use std::sync::Arc;
use std::time::{Duration as StdDuration};

use log::{warn};

use chrono::{DateTime, Utc, Duration};

use bytes::{Bytes, Buf};

use enum_map::{Enum};

use crate::metric;
use crate::stream;

use super::frame;
use super::rtcifier::{LinearRTC, RTCifier};


#[derive(Debug, Clone, Copy, Enum)]
pub enum StreamKind {
	AccelX,
	AccelY,
	AccelZ,
	CompassX,
	CompassY,
	CompassZ,
}

impl From<StreamKind> for metric::DevicePath {
	fn from(k: StreamKind) -> Self {
		let device_type = match k {
			StreamKind::AccelX | StreamKind::AccelY | StreamKind::AccelZ => "lsm303d.accelerometer".into(),
			StreamKind::CompassX | StreamKind::CompassY | StreamKind::CompassZ => "lsm303d.magnetometer".into(),
		};
		let instance = match k {
			StreamKind::AccelX | StreamKind::CompassX => "i2c-1/1d/x".into(),
			StreamKind::AccelY | StreamKind::CompassY => "i2c-1/1d/y".into(),
			StreamKind::AccelZ | StreamKind::CompassZ => "i2c-1/1d/z".into(),
		};
		metric::DevicePath{
			device_type,
			instance,
		}
	}
}

impl TryFrom<frame::SbxMessageType> for StreamKind {
	type Error = ();

	fn try_from(t: frame::SbxMessageType) -> Result<Self, Self::Error> {
		use frame::SbxMessageType::*;
		Ok(match t {
			SensorStreamAccelX => Self::AccelX,
			SensorStreamAccelY => Self::AccelY,
			SensorStreamAccelZ => Self::AccelZ,
			SensorStreamCompassX => Self::CompassX,
			SensorStreamCompassY => Self::CompassY,
			SensorStreamCompassZ => Self::CompassZ,
			_ => return Err(()),
		})
	}
}

#[derive(Debug, Clone)]
pub enum Decompress {
	Init{
		first_sample: u16,
		bitmap: Bytes,
		payload: Bytes,
	},
	Data{
		reference: u16,
		bitmap_byte: u8,
		bitmap_mask: u8,
		bitmap: Bytes,
		payload: Bytes,
	},
	Eof,
}

impl Decompress {
	pub fn new<T: Into<Bytes>>(first_sample: u16, encoded_data: T) -> io::Result<Self> {
		let mut packet = encoded_data.into();
		let bitmap = packet.clone();
		let mut remaining_payload = packet.len();
		let mut bitmap_len = 0;
		while remaining_payload > 0 {
			remaining_payload -= 1;
			bitmap_len += 1;
			if packet.len() == 0 {
				return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "packet ended while scanning bitmap"));
			}
			let next_bitmap_byte = packet.get_u8();

			for bitpos in (0..=7u8).rev() {
				let mask = 1u8 << bitpos;
				debug_assert!(mask != 0);
				let bit = next_bitmap_byte & mask;
				let compressed = bit != 0;
				remaining_payload = match remaining_payload.checked_sub( if compressed { 1 } else { 2 }) {
					Some(v) => v,
					None => {
						return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "packet is too short for given bitmap"))
					}
				};
				if remaining_payload == 0 {
					break;
				}
			}
		}
		let bitmap = bitmap.slice(..bitmap_len);
		Ok(Self::Init{
			first_sample,
			bitmap,
			payload: packet,
		})
	}
}

fn extend_u8(v: u8) -> u16 {
	if v & 0x80u8 != 0 {
		v as u16 | 0xff00
	} else {
		v as u16
	}
}

fn to_sint(v: u16) -> i16 {
	v as i16
}

impl Iterator for Decompress {
	type Item = i16;

	fn next(&mut self) -> Option<i16> {
		match self {
			Self::Init{first_sample, bitmap, payload} => {
				let first_sample_i16 = to_sint(*first_sample);
				*self = if bitmap.len() > 0 {
					let bitmap_byte = bitmap.get_u8();
					Self::Data{
						reference: *first_sample,
						bitmap: bitmap.clone(),
						bitmap_byte,
						bitmap_mask: 0x80,
						payload: payload.clone(),
					}
				} else {
					Self::Eof
				};
				Some(first_sample_i16)
			},
			Self::Data{
					reference,
					ref mut bitmap,
					ref mut bitmap_byte,
					ref mut bitmap_mask,
					ref mut payload} => {
				let bit = (*bitmap_byte) & (*bitmap_mask);
				let compressed = bit != 0;
				let offset = if compressed {
					extend_u8(payload.get_u8())
				} else {
					payload.get_u16_le()
				};
				let v = to_sint(reference.wrapping_add(offset));
				if payload.len() == 0 {
					debug_assert!(bitmap.len() == 0);
					*self = Self::Eof;
					return Some(v);
				}
				*bitmap_mask >>= 1;
				if *bitmap_mask == 0 {
					if bitmap.len() == 0 {
						*self = Self::Eof;
						return Some(v);
					}
					*bitmap_byte = bitmap.get_u8();
					*bitmap_mask = 0x80;
				}
				Some(v)
			},
			Self::Eof => None,
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		match self {
			Self::Init{payload, ..} => {
				((payload.len() + 1) / 2 + 1, Some(payload.len() + 1))
			},
			Self::Data{payload, ..} => {
				((payload.len() + 1) / 2, Some(payload.len()))
			},
			Self::Eof => (0, Some(0)),
		}
	}
}

/// Decode an ongoing stream from a single sensor.
///
/// The decoder decompresses the streams and it also attempts to align their
/// timestamps as precisely as possible.
pub struct StreamDecoder<T: stream::StreamBuffer + Sync + Send + 'static + ?Sized> {
	rtcifier: LinearRTC,
	period: StdDuration,
	scale: metric::Value,
	buffer: Box<T>,
}

impl<T: stream::StreamBuffer + Sync + Send + 'static> StreamDecoder<T> {
	pub fn new(period: StdDuration, buffer: T, scale: metric::Value) -> Self {
		if !buffer.valid_period(period) {
			panic!("slice {} of buffer is not valid for period {:?}", buffer.slice(), period)
		}
		Self{
			rtcifier: LinearRTC::new(15, Duration::seconds(60), 2, 20000),
			period,
			scale,
			buffer: Box::new(buffer),
		}
	}

	pub fn align(&mut self, rtc: DateTime<Utc>, seq: u16) {
		self.rtcifier.align(rtc, seq)
	}

	pub fn ready(&self) -> bool {
		self.rtcifier.ready()
	}

	pub fn reset(&mut self) {
		self.rtcifier.reset()
	}

	pub fn decode<'m>(&mut self, kind: StreamKind, message: &'m frame::SbxStreamMessage) -> io::Result<()> {
		let mut samples = Vec::new();
		let iter = Decompress::new(message.avg, message.coded.clone())?;
		samples.reserve(iter.size_hint().1.unwrap());
		samples.extend(iter);
		samples.shrink_to_fit();
		let t0 = self.rtcifier.map_to_rtc(message.seq);
		match self.buffer.write(&metric::StreamBlock{
			t0,
			seq0: message.seq,
			path: kind.into(),
			period: self.period.clone(),
			scale: self.scale.clone(),
			data: Arc::new(metric::RawData::I16(samples.into())),
		}) {
			Ok(()) => (),
			Err(e) => warn!("sample failed to write to buffer: {}", e),
		};
		Ok(())
	}

	pub fn read_next(&mut self) -> Option<metric::StreamBlock> {
		self.buffer.read_next()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn trivial() {
		let mut dec = Decompress::new(2342u16, &b""[..]).unwrap();
		match dec.next() {
			Some(v) => assert_eq!(v, 2342i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		assert!(dec.next().is_none());
	}

	#[test]
	fn trivial_negative() {
		let mut dec = Decompress::new(65535u16, &b""[..]).unwrap();
		match dec.next() {
			Some(v) => assert_eq!(v, -1i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		assert!(dec.next().is_none());
	}

	#[test]
	fn uncompressed() {
		let mut dec = Decompress::new(2342u16, &b"\x00\x01\x00\xff\xff"[..]).unwrap();
		match dec.next() {
			Some(v) => assert_eq!(v, 2342i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		match dec.next() {
			Some(v) => assert_eq!(v, 2343i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		match dec.next() {
			Some(v) => assert_eq!(v, 2341i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		assert!(dec.next().is_none());
	}

	#[test]
	fn uncompressed_multi_bitmap() {
		let mut dec = Decompress::new(2342u16, &b"\x00\x00\x01\x00\xff\xff\x01\x00\xff\xff\x01\x00\xff\xff\x01\x00\xff\xff\x01\x00\xff\xff"[..]).unwrap();
		match dec.next() {
			Some(v) => assert_eq!(v, 2342i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		for _ in 0..5 {
			match dec.next() {
				Some(v) => assert_eq!(v, 2343i16),
				other => panic!("unexpected next result: {:?}", other),
			}
			match dec.next() {
				Some(v) => assert_eq!(v, 2341i16),
				other => panic!("unexpected next result: {:?}", other),
			}
		}
		assert!(dec.next().is_none());
	}

	#[test]
	fn compressed() {
		let mut dec = Decompress::new(2342u16, &b"\xc0\x01\xff"[..]).unwrap();
		match dec.next() {
			Some(v) => assert_eq!(v, 2342i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		match dec.next() {
			Some(v) => assert_eq!(v, 2343i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		match dec.next() {
			Some(v) => assert_eq!(v, 2341i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		assert!(dec.next().is_none());
	}

	#[test]
	fn compressed_long() {
		let mut dec = Decompress::new(2342u16, &b"\xff\xc0\x01\xff\x01\xff\x01\xff\x01\xff\x01\xff"[..]).unwrap();
		match dec.next() {
			Some(v) => assert_eq!(v, 2342i16),
			other => panic!("unexpected next result: {:?}", other),
		}
		for _ in 0..5 {
			match dec.next() {
				Some(v) => assert_eq!(v, 2343i16),
				other => panic!("unexpected next result: {:?}", other),
			}
			match dec.next() {
				Some(v) => assert_eq!(v, 2341i16),
				other => panic!("unexpected next result: {:?}", other),
			}
		}
		assert!(dec.next().is_none());
	}
}
