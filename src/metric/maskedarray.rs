#![allow(dead_code)]
use std::borrow::Borrow;
use std::ops::{Deref, DerefMut, RangeBounds, Bound, Range};

use bitvec::prelude::{BitVec, Lsb0};

type MaskVec = BitVec<Lsb0, usize>;

pub struct MaskedArray<T> {
	mask: MaskVec,
	values: Vec<T>,
}

impl<T: Clone> MaskedArray<T> {
	pub fn masked_with_value(size: usize, v: T) -> Self {
		let mut values = Vec::with_capacity(size);
		let mut mask = BitVec::with_capacity(size);
		mask.resize(size, false);
		values.resize(size, v);
		debug_assert!(values.len() == values.capacity());
		debug_assert!(values.len() == size);
		debug_assert!(mask.len() == size);
		Self{
			mask,
			values,
		}
	}
}

impl<T: Default> MaskedArray<T> {
	pub fn masked_with_default(size: usize) -> Self {
		let mut values = Vec::with_capacity(size);
		let mut mask = BitVec::with_capacity(size);
		mask.resize(size, false);
		for _ in 0..size {
			values.push(T::default())
		}
		debug_assert!(values.len() == values.capacity());
		debug_assert!(values.len() == size);
		debug_assert!(mask.len() == size);
		Self{
			mask,
			values,
		}
	}
}

// TODO: use std::slice::range after stabilization
// stolen from https://github.com/rust-lang/rust/pull/81154/files
fn rangeify(l: usize, range: impl RangeBounds<usize>) -> Range<usize> {
	let len = l;

	let start: Bound<&usize> = range.start_bound();
	let start = match start {
		Bound::Included(&start) => start,
		Bound::Excluded(start) => {
			start.checked_add(1).unwrap()
		}
		Bound::Unbounded => 0,
	};

	let end: Bound<&usize> = range.end_bound();
	let end = match end {
		Bound::Included(end) => {
			end.checked_add(1).unwrap()
		}
		Bound::Excluded(&end) => end,
		Bound::Unbounded => len,
	};

	if start > end {
		panic!("start is greater than end");
	}
	if end > len {
		panic!("out of bounds");
	}

	Range { start, end }
}

impl<T> MaskedArray<T> {
	#[inline]
	pub fn len(&self) -> usize {
		debug_assert!(self.mask.len() == self.values.len());
		self.values.len()
	}

	pub fn unmask(&mut self, range: impl RangeBounds<usize>) {
		let len = self.mask.len();
		self.mask[rangeify(len, range)].set_all(true);
	}

	pub fn mask(&mut self, range: impl RangeBounds<usize>) {
		let len = self.mask.len();
		self.mask[rangeify(len, range)].set_all(false);
	}

	#[inline]
	pub fn masked(&self, index: usize) -> bool {
		!self.mask[index]
	}

	#[inline]
	pub fn iter_mask<'x>(&'x self) -> bitvec::slice::Iter<'x, Lsb0, usize> {
		self.mask.iter()
	}

	#[inline]
	pub fn iter_mask_mut<'x>(&'x mut self) -> bitvec::slice::IterMut<'x, Lsb0, usize> {
		self.mask.iter_mut()
	}

	pub fn write_from<I: Iterator<Item = T>>(&mut self, at: usize, iter: I) {
		let mut n = 0;
		for (offset, (src, dest)) in iter.zip(self.values[at..].iter_mut()).enumerate() {
			let index = offset.checked_add(at).expect("proper index");
			n += 1;
			*dest = src;
		}
		self.unmask(at..(at+n));
	}
}

impl<T: Clone> MaskedArray<T> {
	pub fn write_clone<I: Iterator<Item = V>, V: Borrow<T>>(&mut self, at: usize, iter: I) {
		let mut n = 0;
		for (offset, (src, dest)) in iter.zip(self.values[at..].iter_mut()).enumerate() {
			let index = offset.checked_add(at).expect("proper index");
			n += 1;
			*dest = src.borrow().clone();
		}
		self.unmask(at..(at+n));
	}
}

impl<T> Deref for MaskedArray<T> {
	type Target = [T];

	fn deref(&self) -> &[T] {
		&self.values[..]
	}
}

impl<T> DerefMut for MaskedArray<T> {
	fn deref_mut(&mut self) -> &mut [T] {
		&mut self.values[..]
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_basics() {
		let arr = MaskedArray::masked_with_value(16, 123u8);
		assert_eq!(arr.len(), 16);
		for v in arr.iter() {
			assert_eq!(*v, 123u8);
		}
		for m in arr.iter_mask() {
			assert_eq!(*m, false);
		}
	}

	fn test_write_from_unmasks() {
		let mut arr = MaskedArray::masked_with_value(16, 0u8);
		let mut src = vec![1u8, 2u8, 3u8];
		arr.write_from(2, src.drain(..));
		assert_eq!(arr[2], 1u8);
		assert_eq!(arr[3], 2u8);
		assert_eq!(arr[4], 3u8);
		assert!(arr.masked(1));
		assert!(!arr.masked(2));
		assert!(!arr.masked(3));
		assert!(!arr.masked(4));
		assert!(arr.masked(5));
	}

	fn test_write_clone_unmasks() {
		let mut arr = MaskedArray::masked_with_value(16, 0u8);
		let mut src = vec![1u8, 2u8, 3u8];
		arr.write_clone(2, src.iter());
		assert_eq!(arr[2], 1u8);
		assert_eq!(arr[3], 2u8);
		assert_eq!(arr[4], 3u8);
		assert!(arr.masked(1));
		assert!(!arr.masked(2));
		assert!(!arr.masked(3));
		assert!(!arr.masked(4));
		assert!(arr.masked(5));
	}
}
