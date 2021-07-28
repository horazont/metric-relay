use std::cmp::Ordering;
use std::ops::Deref;

use bytes::{Bytes, BytesMut};

use super::serial::SerialNumber;
use super::frame::{DataFrame, RawDataFrameHeader};

#[derive(Debug, Clone)]
pub struct SendQueue {
	max_size: usize,
	q: Vec<(SerialNumber, Bytes)>,
	next_sn: SerialNumber,
	min_sn: SerialNumber,
}

impl SendQueue {
	pub fn new(max_size: usize, first_sn: SerialNumber) -> SendQueue {
		assert!(max_size > 0);
		let mut q = Vec::new();
		q.reserve(max_size);
		SendQueue{
			max_size,
			q,
			next_sn: first_sn,
			min_sn: first_sn,
		}
	}

	#[allow(dead_code)]
	pub fn max_size(&self) -> usize {
		self.max_size
	}

	pub fn min_sn(&self) -> SerialNumber {
		self.min_sn
	}

	#[allow(dead_code)]
	pub fn next_sn(&self) -> SerialNumber {
		self.next_sn
	}

	pub fn len(&self) -> usize {
		self.q.len()
	}

	fn push_into_queue(&mut self, sn: SerialNumber, frame: Bytes) {
		if self.q.len() >= self.max_size {
			// drop the oldest one
			self.q.remove(0);
			self.min_sn = if self.q.len() > 0 {
				self.q[0].0
			} else {
				// if the max size is one, the min avail sn will be the one we will now push
				sn
			}
		}
		self.q.push((sn, frame))
	}

	pub fn push<T: Into<Bytes>>(&mut self, payload: T) {
		let payload = payload.into();
		assert!(payload.len() <= 255);
		let mut buf = BytesMut::new();
		let sn = self.next_sn;
		buf.reserve(payload.len() + RawDataFrameHeader::RAW_LEN);
		DataFrame{
			sn: self.next_sn,
			payload: payload,
		}.write(&mut buf).unwrap();

		self.push_into_queue(sn, buf.freeze());
		self.next_sn = self.next_sn + 1;
	}

	#[allow(dead_code)]
	pub fn frames(&self) -> &Vec<(SerialNumber, Bytes)> {
		&self.q
	}

	// Discard a frame from the send queue, if it exists.
	pub fn discard(&mut self, sn: SerialNumber) {
		match self.q.binary_search_by(|e| {
			e.0.partial_cmp(&sn).unwrap_or(Ordering::Equal)
		}) {
			// element exists, delete it
			Ok(index) => {
				self.q.remove(index);
				if index == 0 {
					// when removing the first element, we need to update the min_sn, as that has definitely changed
					self.min_sn = if self.q.len() > 0 {
						self.q[0].0
					} else {
						// if there is nothing left in the queue, the min_sn becomes the next_sn
						self.next_sn
					}
				}
			},
			// element does not exist, that’s ok
			Err(_) => (),
		};
	}

	// Discard all frames up to and including a given serial number
	pub fn discard_up_to_incl(&mut self, sn: SerialNumber) {
		self.q.retain(|e| {
			e.0 > sn
		});
		self.min_sn = if self.q.len() > 0 {
			self.q[0].0
		} else {
			self.next_sn
		}
	}
}

impl Deref for SendQueue {
	type Target = [(SerialNumber, Bytes)];

	fn deref(&self) -> &Self::Target {
		&self.q[..]
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_init() {
		let q = SendQueue::new(16, 2342u16.into());
		assert_eq!(q.max_size(), 16);
		assert_eq!(q.min_sn(), 2342u16.into());
		assert_eq!(q.next_sn(), 2342u16.into());
		assert_eq!(q.len(), 0);
	}

	#[test]
	fn test_push_increases_next_sn() {
		let mut q = SendQueue::new(16, 2342u16.into());
		q.push(&b""[..]);
		assert_eq!(q.min_sn(), 2342u16.into());
		assert_eq!(q.next_sn(), 2343u16.into());
		assert_eq!(q.len(), 1);
	}

	#[test]
	fn test_push_limits_to_max_size_and_drops_oldest() {
		let mut q = SendQueue::new(4, 2342u16.into());
		q.push(&b"foo1"[..]);
		q.push(&b"foo2"[..]);
		q.push(&b"foo3"[..]);
		q.push(&b"foo4"[..]);
		q.push(&b"foo5"[..]);
		assert_eq!(q.min_sn(), 2343u16.into());
		assert_eq!(q.next_sn(), 2347u16.into());
		assert_eq!(q.len(), 4);
		// slicing to remove the data frame header
		assert_eq!(&q.frames()[0].1[3..], &b"foo2"[..]);
		assert_eq!(&q.frames()[1].1[3..], &b"foo3"[..]);
		assert_eq!(&q.frames()[2].1[3..], &b"foo4"[..]);
		assert_eq!(&q.frames()[3].1[3..], &b"foo5"[..]);
	}

	#[test]
	fn test_discard_drops_single_frame_if_it_exists() {
		let mut q = SendQueue::new(4, 2342u16.into());
		q.push(&b"foo1"[..]);
		q.push(&b"foo2"[..]);
		q.push(&b"foo3"[..]);
		q.push(&b"foo4"[..]);
		q.discard(2344u16.into());
		assert_eq!(q.min_sn(), 2342u16.into());
		assert_eq!(q.next_sn(), 2346u16.into());
		assert_eq!(q.len(), 3);
		// slicing to remove the data frame header
		assert_eq!(&q.frames()[0].1[3..], &b"foo1"[..]);
		assert_eq!(&q.frames()[1].1[3..], &b"foo2"[..]);
		assert_eq!(&q.frames()[2].1[3..], &b"foo4"[..]);
	}

	#[test]
	fn test_discard_increases_min_sn_if_first_frame_is_dropped() {
		let mut q = SendQueue::new(4, 2342u16.into());
		q.push(&b"foo1"[..]);
		q.push(&b"foo2"[..]);
		q.push(&b"foo3"[..]);
		q.push(&b"foo4"[..]);
		q.discard(2343u16.into());
		q.discard(2342u16.into());
		assert_eq!(q.min_sn(), 2344u16.into());
		assert_eq!(q.next_sn(), 2346u16.into());
		assert_eq!(q.len(), 2);
		// slicing to remove the data frame header
		assert_eq!(&q.frames()[0].1[3..], &b"foo3"[..]);
		assert_eq!(&q.frames()[1].1[3..], &b"foo4"[..]);
	}

	#[test]
	fn test_discard_up_to_incl_discards_matching_elements() {
		let mut q = SendQueue::new(4, 2342u16.into());
		q.push(&b"foo1"[..]);
		q.push(&b"foo2"[..]);
		q.push(&b"foo3"[..]);
		q.push(&b"foo4"[..]);
		q.discard_up_to_incl(2343u16.into());
		assert_eq!(q.min_sn(), 2344u16.into());
		assert_eq!(q.next_sn(), 2346u16.into());
		assert_eq!(q.len(), 2);
		// slicing to remove the data frame header
		assert_eq!(&q.frames()[0].1[3..], &b"foo3"[..]);
		assert_eq!(&q.frames()[1].1[3..], &b"foo4"[..]);
	}

	#[test]
	fn test_discard_up_to_incl_sets_correct_min_sn_when_everything_is_dropped() {
		let mut q = SendQueue::new(4, 2342u16.into());
		q.push(&b"foo1"[..]);
		q.push(&b"foo2"[..]);
		q.push(&b"foo3"[..]);
		q.push(&b"foo4"[..]);
		q.discard_up_to_incl(2350u16.into());
		assert_eq!(q.min_sn(), 2346u16.into());
		assert_eq!(q.next_sn(), 2346u16.into());
		assert_eq!(q.len(), 0);
	}

	#[test]
	fn test_discard_up_to_incl_sets_correct_min_sn_with_gaps() {
		let mut q = SendQueue::new(4, 2342u16.into());
		q.push(&b"foo1"[..]);
		q.push(&b"foo2"[..]);
		q.push(&b"foo3"[..]);
		q.push(&b"foo4"[..]);
		q.discard(2344u16.into());
		q.discard_up_to_incl(2343u16.into());
		assert_eq!(q.min_sn(), 2345u16.into());
		assert_eq!(q.next_sn(), 2346u16.into());
		assert_eq!(q.len(), 1);
	}
}
