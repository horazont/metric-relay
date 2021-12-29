use super::frame::DataFrame;
use crate::serial::SerialNumber;
use bytes::Bytes;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct RecvQueue {
	max_size: usize,
	lowest_sn: SerialNumber,
	q: Vec<DataFrame>,
}

impl RecvQueue {
	pub fn new(max_size: usize, lowest_sn: SerialNumber) -> RecvQueue {
		let mut q = Vec::new();
		q.reserve(max_size);
		RecvQueue {
			max_size,
			lowest_sn,
			q,
		}
	}

	/// Put a payload into the buffer.
	///
	/// If the payload is late, it may be silently discarded.
	pub fn set(&mut self, sn: SerialNumber, data: Bytes) {
		match sn.checked_sub(self.lowest_sn) {
			// waaay too far ahead in the future. maybe.
			None => return,
			Some(v) => {
				if v < 0 {
					// too late, sorry
					return;
				} else if v as usize >= self.max_size {
					// we cannot guarantee that we can hold all necessary frames in memory
					return;
				}
				// otherwise its ok
			}
		};
		let insert_index = match self
			.q
			.binary_search_by(|e| e.sn().partial_cmp(&sn).unwrap_or(Ordering::Equal))
		{
			// element exists already
			Ok(_) => return,
			Err(i) => i,
		};
		self.q.insert(insert_index, DataFrame { sn, payload: data });
	}

	/// Try to read the next frame from the queue.
	pub fn try_read(&mut self) -> Option<DataFrame> {
		if self.q.len() == 0 {
			return None;
		}
		let first = &self.q[0];
		if first.sn() <= self.lowest_sn {
			drop(first);
			let result = Some(self.q.remove(0));
			self.lowest_sn = self.lowest_sn + 1;
			result
		} else {
			None
		}
	}

	/// Mark all serial numbers up to but **excluding** sn to be unreceivable.
	pub fn mark_unreceivable_up_to(&mut self, sn: SerialNumber) {
		if sn > self.lowest_sn {
			self.lowest_sn = sn;
		}
	}

	/// Flush the entire reception queue and reset the state
	pub fn flush(&mut self, lowest_sn: SerialNumber) -> Vec<DataFrame> {
		self.lowest_sn = lowest_sn;
		self.q.split_off(0)
	}

	#[allow(dead_code)]
	pub fn lowest_sn(&self) -> SerialNumber {
		self.lowest_sn
	}

	pub fn max_consecutive_sn(&self) -> SerialNumber {
		let mut next_sn = self.lowest_sn;
		for ref frame in self.q.iter() {
			if frame.sn() != next_sn {
				return next_sn - 1;
			}
			next_sn = next_sn + 1
		}
		next_sn - 1
	}

	#[allow(dead_code)]
	pub fn len(&self) -> usize {
		self.q.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_setting_non_next_sn_does_not_make_packet_available() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(1u16.into(), (&b""[..]).into());
		match q.try_read() {
			None => (),
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_setting_next_sn_makes_packet_available_for_reading() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(0u16.into(), (&b"foo"[..]).into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 0u16.into());
				assert_eq!(payload, &b"foo"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_inserting_next_sn_after_non_next_makes_both_available() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(1u16.into(), (&b"foo1"[..]).into());
		q.set(0u16.into(), (&b"foo0"[..]).into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 0u16.into());
				assert_eq!(payload, &b"foo0"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 1u16.into());
				assert_eq!(payload, &b"foo1"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_setting_previous_sn_drops() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(0u16.into(), (&b"foo"[..]).into());
		q.set(1u16.into(), (&b"foo"[..]).into());
		q.try_read().unwrap();
		q.try_read().unwrap();
		q.set(0u16.into(), (&b"foo0"[..]).into());
		match q.try_read() {
			None => (),
			other => panic!("unexpected read result: {:?}", other),
		}

		q.set(2u16.into(), (&b"foo2"[..]).into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 2u16.into());
				assert_eq!(payload, &b"foo2"[..]);
			}
			other => panic!("unexpected read result: {:?} from {:?}", other, q),
		}
	}

	#[test]
	fn test_marking_missing_as_unreceivable_makes_packets_available() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(1u16.into(), (&b"foo"[..]).into());
		q.mark_unreceivable_up_to(1u16.into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 1u16.into());
				assert_eq!(payload, &b"foo"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_marking_missing_as_unreceivable_makes_packets_available_even_with_gaps() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(1u16.into(), (&b"foo"[..]).into());
		q.mark_unreceivable_up_to(5u16.into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 1u16.into());
				assert_eq!(payload, &b"foo"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_marking_unreceivable_up_to_does_not_move_backwards() {
		let mut q = RecvQueue::new(16, 0.into());
		q.set(10u16.into(), (&b"foo"[..]).into());
		q.mark_unreceivable_up_to(5u16.into());
		q.mark_unreceivable_up_to(0u16.into());
		q.set(0u16.into(), (&b"foo"[..]).into());
		match q.try_read() {
			None => (),
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_setting_with_wrapping_sn() {
		let mut q = RecvQueue::new(16, 65534.into());
		q.set(65534u16.into(), (&b"foo-2"[..]).into());
		q.set(65535u16.into(), (&b"foo-1"[..]).into());
		q.set(0u16.into(), (&b"foo0"[..]).into());
		q.set(1u16.into(), (&b"foo1"[..]).into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 65534u16.into());
				assert_eq!(payload, &b"foo-2"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 65535u16.into());
				assert_eq!(payload, &b"foo-1"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 0u16.into());
				assert_eq!(payload, &b"foo0"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 1u16.into());
				assert_eq!(payload, &b"foo1"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_flush_returns_all_in_order_and_sets_lowest_sn() {
		let mut q = RecvQueue::new(2048, 65500.into());
		q.set(65535u16.into(), (&b"foo-1"[..]).into());
		q.set(10u16.into(), (&b"foo10"[..]).into());
		q.set(2u16.into(), (&b"foo2"[..]).into());
		q.set(5u16.into(), (&b"foo5"[..]).into());
		let frames = q.flush(30u16.into());
		assert_eq!(frames.len(), 4);
		assert_eq!(frames[0].sn(), 65535u16.into());
		assert_eq!(frames[1].sn(), 2u16.into());
		assert_eq!(frames[2].sn(), 5u16.into());
		assert_eq!(frames[3].sn(), 10u16.into());

		q.set(30u16.into(), (&b"foo30"[..]).into());
		match q.try_read() {
			Some(DataFrame { sn, payload }) => {
				assert_eq!(sn, 30u16.into());
				assert_eq!(payload, &b"foo30"[..]);
			}
			other => panic!("unexpected read result: {:?}", other),
		}
	}

	#[test]
	fn test_max_consecutive_sn_counts_correctly() {
		let mut q = RecvQueue::new(16, 65534.into());
		assert_eq!(q.max_consecutive_sn(), 65533.into());
		q.set(65535u16.into(), (&b"foo-1"[..]).into());
		assert_eq!(q.max_consecutive_sn(), 65533.into());
		q.set(65534u16.into(), (&b"foo-2"[..]).into());
		assert_eq!(q.max_consecutive_sn(), 65535.into());
		q.set(0u16.into(), (&b"foo0"[..]).into());
		assert_eq!(q.max_consecutive_sn(), 0.into());
		q.set(1u16.into(), (&b"foo1"[..]).into());
		assert_eq!(q.max_consecutive_sn(), 1.into());
	}
}
