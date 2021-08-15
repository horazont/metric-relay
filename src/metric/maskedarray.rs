#![allow(dead_code)]
use std::borrow::Borrow;
use std::cmp::PartialEq;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut, RangeBounds, Bound, Range};

use bitvec::prelude::{BitVec, Lsb0};

#[cfg(feature = "metric-serde")]
use serde_derive::{Deserialize, Serialize};

type MaskVec = BitVec<Lsb0, usize>;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
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

impl<T: PartialEq> PartialEq<MaskedArray<T>> for MaskedArray<T> {
	fn eq(&self, other: &Self) -> bool {
		// the equality must only take into concern the unmasked values.
		// if there is a difference in masking, the arrays are unequal
		// but if the length are unequal, we can exit right away
		if self.len() != other.len() {
			return false
		}
		for ((lm, lv), (rm, rv)) in self.iter_mask().zip(self.iter()).zip(other.iter_mask().zip(other.iter())) {
			if !*lm && !*rm {
				continue
			}
			if *lm != *rm || lv != rv {
				return false
			}
		}
		true
	}
}

impl<T: Eq> Eq for MaskedArray<T> {}

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
	pub fn from_unmasked_vec(values: Vec<T>) -> Self {
		let mut mask = BitVec::with_capacity(values.len());
		mask.resize(values.len(), true);
		Self{
			mask,
			values,
		}
	}

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

	pub fn get_mask(&self) -> &MaskVec {
		&self.mask
	}
}

impl<T> FromIterator<T> for MaskedArray<T> {
	fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
		let mut vec: Vec<T> = iter.into_iter().collect();
		vec.shrink_to_fit();
		Self::from_unmasked_vec(vec)
	}
}

impl<T> From<Vec<T>> for MaskedArray<T> {
	fn from(other: Vec<T>) -> Self {
		Self::from_unmasked_vec(other)
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

#[derive(Debug, Clone)]
pub struct MaskedArrayWriter<T> {
	inner: MaskedArray<T>,
	cursor: usize,
}

impl<T> MaskedArrayWriter<T> {
	pub fn wrap(inner: MaskedArray<T>, at: usize) -> Self {
		Self{
			inner,
			cursor: at,
		}
	}

	pub fn into_inner(self) -> MaskedArray<T> {
		self.inner
	}

	pub fn remaining_mut(&self) -> usize {
		self.inner.len().checked_sub(self.cursor).unwrap_or(0)
	}

	pub fn cursor(&self) -> usize {
		self.cursor
	}

	pub fn capacity(&self) -> usize {
		self.inner.len()
	}

	pub fn get(&self) -> &MaskedArray<T> {
		&self.inner
	}

	pub fn write(&mut self, v: T) -> usize {
		if self.cursor >= self.inner.len() {
			return 0
		}
		self.inner[self.cursor] = v;
		self.inner.unmask(self.cursor..self.cursor+1);
		self.cursor += 1;
		1
	}

	pub fn write_from<I: Iterator<Item = T>>(&mut self, iter: I) -> usize {
		let mut n = 0usize;
		for src in iter {
			let index = match n.checked_add(self.cursor) {
				Some(v) => v,
				None => break,
			};
			if index >= self.inner.len() {
				break
			}
			self.inner[index] = src;
			n += 1;
		}
		let end = self.cursor.checked_add(n).unwrap();
		self.inner.unmask(self.cursor..end);
		self.cursor = end;
		n
	}

	pub fn seek(&mut self, offset: isize) {
		self.cursor = if offset < 0 {
			self.cursor.checked_sub((offset as usize).wrapping_neg()).unwrap()
		} else {
			self.cursor.checked_add(offset as usize).unwrap()
		}
	}

	pub fn setpos(&mut self, at: usize) {
		if at > self.inner.len() {
			panic!("position {} out of bounds for array length {}", at, self.inner.len())
		}
		// setting it to len() is valid to make remaining_mut zero
		self.cursor = at;
	}
}

impl<T> From<MaskedArray<T>> for MaskedArrayWriter<T> {
	fn from(other: MaskedArray<T>) -> Self {
		Self::wrap(other, 0)
	}
}

impl<'a, T: Copy + 'a> MaskedArrayWriter<T> {
	pub fn write_copy<I: Iterator<Item = &'a T>>(&mut self, iter: I) -> usize {
		self.write_from(iter.map(|x| { *x }))
	}

	pub fn copy_from_slice(&mut self, sl: &'a [T]) {
		let end = match self.cursor.checked_add(sl.len()) {
			Some(v) if v > self.inner.len() => panic!("copying from slice would cause out of bounds write"),
			Some(v) => v,
			None => panic!("end point of write overflows usize"),
		};
		let dest_sl: &mut [T] = &mut self.inner[self.cursor..end];
		dest_sl.copy_from_slice(sl);
		self.inner.unmask(self.cursor..end);
		self.cursor = end;
	}
}

impl<'a, T: Clone + 'a> MaskedArrayWriter<T> {
	pub fn write_clone<I: Iterator<Item = &'a T>>(&mut self, iter: I) -> usize {
		self.write_from(iter.map(|x| { x.clone() }))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
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

	#[test]
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

	#[test]
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

	#[test]
	fn test_cursor_write() {
		let mut w = MaskedArrayWriter::wrap(MaskedArray::masked_with_value(16, 0u8), 0);
		assert_eq!(w.write_copy((&[1u8, 2u8][..]).iter()), 2);

		{
			let arr = w.get();
			assert_eq!(arr[0], 1);
			assert_eq!(arr[1], 2);
			assert_eq!(arr[2], 0);
			assert!(arr.get_mask()[..2].all());
			assert!(!arr.get_mask()[3..].any());
		}

		assert_eq!(w.write_copy((&[10u8, 11u8, 12u8, 13u8][..]).iter()), 4);
		assert_eq!(w.write_copy((&[10u8, 11u8, 12u8, 13u8][..]).iter()), 4);
		assert_eq!(w.write_copy((&[10u8, 11u8, 12u8, 13u8][..]).iter()), 4);
		assert_eq!(w.write_copy((&[10u8, 11u8, 12u8, 13u8][..]).iter()), 2);
		let arr = w.into_inner();
		assert_eq!(arr[0], 1);
		assert_eq!(arr[1], 2);
		assert_eq!(arr[0*4 + 2], 10);
		assert_eq!(arr[0*4 + 3], 11);
		assert_eq!(arr[0*4 + 4], 12);
		assert_eq!(arr[0*4 + 5], 13);
		assert_eq!(arr[1*4 + 2], 10);
		assert_eq!(arr[1*4 + 3], 11);
		assert_eq!(arr[1*4 + 4], 12);
		assert_eq!(arr[1*4 + 5], 13);
		assert_eq!(arr[2*4 + 2], 10);
		assert_eq!(arr[2*4 + 3], 11);
		assert_eq!(arr[2*4 + 4], 12);
		assert_eq!(arr[2*4 + 5], 13);
		assert_eq!(arr[3*4 + 2], 10);
		assert_eq!(arr[3*4 + 3], 11);
		assert!(arr.get_mask().all());
	}

	#[test]
	fn test_cursor_copy_from_slice() {
		let mut w = MaskedArrayWriter::wrap(MaskedArray::masked_with_value(16, 0u8), 0);
		w.copy_from_slice(&[1u8, 2u8][..]);

		{
			let arr = w.get();
			assert_eq!(arr[0], 1);
			assert_eq!(arr[1], 2);
			assert_eq!(arr[2], 0);
			assert!(arr.get_mask()[..2].all());
			assert!(!arr.get_mask()[3..].any());
		}

		w.copy_from_slice(&[10u8, 11u8, 12u8, 13u8][..]);
		w.copy_from_slice(&[10u8, 11u8, 12u8, 13u8][..]);
		w.copy_from_slice(&[10u8, 11u8, 12u8, 13u8][..]);
		w.copy_from_slice(&[10u8, 11u8, 12u8, 13u8][..2]);
		let arr = w.into_inner();
		assert_eq!(arr[0], 1);
		assert_eq!(arr[1], 2);
		assert_eq!(arr[0*4 + 2], 10);
		assert_eq!(arr[0*4 + 3], 11);
		assert_eq!(arr[0*4 + 4], 12);
		assert_eq!(arr[0*4 + 5], 13);
		assert_eq!(arr[1*4 + 2], 10);
		assert_eq!(arr[1*4 + 3], 11);
		assert_eq!(arr[1*4 + 4], 12);
		assert_eq!(arr[1*4 + 5], 13);
		assert_eq!(arr[2*4 + 2], 10);
		assert_eq!(arr[2*4 + 3], 11);
		assert_eq!(arr[2*4 + 4], 12);
		assert_eq!(arr[2*4 + 5], 13);
		assert_eq!(arr[3*4 + 2], 10);
		assert_eq!(arr[3*4 + 3], 11);
		assert!(arr.get_mask().all());
	}
}
