#![allow(dead_code)]
use std::borrow::Borrow;
use std::cmp::PartialEq;
use std::iter::FromIterator;
use std::ops::{Bound, Deref, DerefMut, Range, RangeBounds};

use bitvec::prelude::{BitSlice, BitVec, Lsb0};

#[cfg(feature = "metric-serde")]
use serde_derive::{Deserialize, Serialize};

type MaskVec = BitVec<usize, Lsb0>;
type MaskSlice = BitSlice<usize, Lsb0>;

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
		Self { mask, values }
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
		Self { mask, values }
	}
}

impl<T: PartialEq> PartialEq<MaskedArray<T>> for MaskedArray<T> {
	fn eq(&self, other: &Self) -> bool {
		// the equality must only take into concern the unmasked values.
		// if there is a difference in masking, the arrays are unequal
		// but if the length are unequal, we can exit right away
		if self.len() != other.len() {
			return false;
		}
		for ((lm, lv), (rm, rv)) in self
			.iter_mask()
			.zip(self.iter())
			.zip(other.iter_mask().zip(other.iter()))
		{
			if !*lm && !*rm {
				continue;
			}
			if *lm != *rm || lv != rv {
				return false;
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
		Bound::Excluded(start) => start.checked_add(1).unwrap(),
		Bound::Unbounded => 0,
	};

	let end: Bound<&usize> = range.end_bound();
	let end = match end {
		Bound::Included(end) => end.checked_add(1).unwrap(),
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
		Self { mask, values }
	}

	pub fn with_data<O>(&self, other: Vec<O>) -> MaskedArray<O> {
		assert!(other.len() == self.values.len());
		debug_assert!(self.values.len() == self.mask.len());
		MaskedArray {
			mask: self.mask.clone(),
			values: other,
		}
	}

	#[inline]
	pub fn len(&self) -> usize {
		debug_assert!(self.mask.len() == self.values.len());
		self.values.len()
	}

	pub fn unmask(&mut self, range: impl RangeBounds<usize>) {
		let len = self.mask.len();
		self.mask[rangeify(len, range)].fill(true);
	}

	pub fn mask(&mut self, range: impl RangeBounds<usize>) {
		let len = self.mask.len();
		self.mask[rangeify(len, range)].fill(false);
	}

	#[inline]
	pub fn masked(&self, index: usize) -> bool {
		!self.mask[index]
	}

	#[inline]
	pub fn iter_mask<'x>(&'x self) -> bitvec::slice::Iter<'x, usize, Lsb0> {
		self.mask.iter()
	}

	#[inline]
	pub fn iter_mask_mut<'x>(&'x mut self) -> bitvec::slice::IterMut<'x, usize, Lsb0> {
		self.mask.iter_mut()
	}

	pub fn write_from<I: Iterator<Item = T>>(&mut self, at: usize, iter: I) {
		let mut n = 0usize;
		for (src, dest) in iter.zip(self.values[at..].iter_mut()) {
			n = n.checked_add(1).unwrap();
			*dest = src;
		}
		self.unmask(at..(at + n));
	}

	pub fn get_mask(&self) -> &MaskVec {
		&self.mask
	}

	pub fn iter_unmasked<'a>(&'a self, r: impl RangeBounds<usize>) -> Unmasked<'a, T> {
		let r = rangeify(self.mask.len(), r);
		Unmasked {
			mask: &self.mask[r.clone()],
			values: &self.values[r],
		}
	}

	pub fn iter_unmasked_mut<'a>(&'a mut self, r: impl RangeBounds<usize>) -> UnmaskedMut<'a, T> {
		let r = rangeify(self.mask.len(), r);
		UnmaskedMut {
			mask: &self.mask[r.clone()],
			values: &mut self.values[r],
		}
	}

	pub fn iter_unmasked_enumerated<'a>(&'a self) -> UnmaskedEnumerated<'a, T> {
		UnmaskedEnumerated {
			mask: &self.mask[..],
			values: &self.values[..],
			at: 0,
		}
	}

	pub fn unmasked_chunks<'a>(&'a self, sz: usize) -> UnmaskedChunks<'a, T> {
		UnmaskedChunks {
			array: &self,
			chunk_size: sz,
			offset: 0,
		}
	}

	pub fn iter_filled<'a>(&'a self, r: impl RangeBounds<usize>, fill: &'a T) -> Filled<'a, T> {
		let len = self.mask.len();
		let r = rangeify(len, r);
		Filled {
			mask: &self.mask[r.clone()],
			values: &self.values[r],
			fill,
		}
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
		let mut n = 0usize;
		for (src, dest) in iter.zip(self.values[at..].iter_mut()) {
			n = n.checked_add(1).unwrap();
			*dest = src.borrow().clone();
		}
		self.unmask(at..(at + n));
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
		Self { inner, cursor: at }
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
			return 0;
		}
		self.inner[self.cursor] = v;
		self.inner.unmask(self.cursor..self.cursor + 1);
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
				break;
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
			self.cursor
				.checked_sub((offset as usize).wrapping_neg())
				.unwrap()
		} else {
			self.cursor.checked_add(offset as usize).unwrap()
		}
	}

	pub fn setpos(&mut self, at: usize) {
		if at > self.inner.len() {
			panic!(
				"position {} out of bounds for array length {}",
				at,
				self.inner.len()
			)
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
		self.write_from(iter.map(|x| *x))
	}

	pub fn copy_from_slice(&mut self, sl: &'a [T]) {
		let end = match self.cursor.checked_add(sl.len()) {
			Some(v) if v > self.inner.len() => {
				panic!("copying from slice would cause out of bounds write")
			}
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
		self.write_from(iter.map(|x| x.clone()))
	}
}

pub struct Unmasked<'a, T> {
	mask: &'a MaskSlice,
	values: &'a [T],
}

impl<'a, T> Iterator for Unmasked<'a, T> {
	type Item = &'a T;

	fn next(&mut self) -> Option<Self::Item> {
		let mut at = 0usize;
		let nvalues = self.values.len();
		debug_assert!(nvalues == self.mask.len());
		while at < nvalues && !self.mask[at] {
			at += 1;
		}
		if at >= nvalues {
			self.mask = MaskSlice::empty();
			self.values = &[];
			None
		} else {
			// we need to exclude the selected item from the head slice, hence we add one
			// this is okay, at < nvalues && nvalues == len and it is valid for at to be equal to len according to the split_at docs (the second slice will then simply be empty)
			let splitpoint = at + 1;
			debug_assert!(self.mask[at]);
			self.mask = self.mask.split_at(splitpoint).1;
			let (head, tail) = std::mem::take(&mut self.values).split_at(splitpoint);
			self.values = tail;
			Some(&head[at])
		}
	}
}

pub struct UnmaskedMut<'a, T> {
	mask: &'a MaskSlice,
	values: &'a mut [T],
}

impl<'a, T> Iterator for UnmaskedMut<'a, T> {
	type Item = &'a mut T;

	fn next(&mut self) -> Option<Self::Item> {
		let mut at = 0usize;
		let nvalues = self.values.len();
		debug_assert!(nvalues == self.mask.len());
		while at < nvalues && !self.mask[at] {
			at += 1;
		}
		if at >= nvalues {
			self.mask = MaskSlice::empty();
			self.values = &mut [];
			None
		} else {
			// we need to exclude the selected item from the head slice, hence we add one
			// this is okay, at < nvalues && nvalues == len and it is valid for at to be equal to len according to the split_at docs (the second slice will then simply be empty)
			let splitpoint = at + 1;
			debug_assert!(self.mask[at]);
			self.mask = self.mask.split_at(splitpoint).1;
			let (head, tail) = std::mem::take(&mut self.values).split_at_mut(splitpoint);
			self.values = tail;
			Some(&mut head[at])
		}
	}
}

pub struct UnmaskedEnumerated<'a, T> {
	mask: &'a MaskSlice,
	values: &'a [T],
	at: usize,
}

impl<'a, T> Iterator for UnmaskedEnumerated<'a, T> {
	type Item = (usize, &'a T);

	fn next(&mut self) -> Option<Self::Item> {
		let nvalues = self.values.len();
		debug_assert!(nvalues == self.mask.len());
		let mut at = self.at;
		while at < nvalues && !self.mask[at] {
			at += 1;
		}
		if at >= nvalues {
			self.at = nvalues;
			None
		} else {
			self.at = at + 1;
			debug_assert!(self.mask[at]);
			Some((at, &self.values[at]))
		}
	}
}

pub struct UnmaskedChunks<'a, T> {
	array: &'a MaskedArray<T>,
	chunk_size: usize,
	offset: usize,
}

impl<'a, T> Iterator for UnmaskedChunks<'a, T> {
	type Item = Unmasked<'a, T>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.offset >= self.array.len() {
			return None;
		}

		let rbegin = self.offset;
		let next = self.offset + self.chunk_size;
		let rend = next.min(self.array.len());
		let r = rbegin..rend;

		let result = Unmasked {
			mask: &self.array.mask[r.clone()],
			values: &self.array.values[r],
		};
		self.offset = next;
		Some(result)
	}
}

pub struct Filled<'a, T> {
	mask: &'a MaskSlice,
	values: &'a [T],
	fill: &'a T,
}

impl<'a, T> Iterator for Filled<'a, T> {
	type Item = &'a T;

	fn next(&mut self) -> Option<Self::Item> {
		debug_assert!(self.mask.len() == self.values.len());
		match self.mask.split_first() {
			Some((valid, mtail)) => {
				let (value, vtail) = self.values.split_first().unwrap();
				self.mask = mtail;
				self.values = vtail;
				if *valid {
					Some(value)
				} else {
					Some(&self.fill)
				}
			}
			None => None,
		}
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
		let src = vec![1u8, 2u8, 3u8];
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
	fn test_iter_unmasked() {
		let mut arr = MaskedArray::masked_with_value(16, 2342u16);
		assert_eq!(
			Vec::<u16>::new(),
			arr.iter_unmasked(..).map(|x| { *x }).collect::<Vec<_>>()
		);
		arr.unmask(10..11);
		assert_eq!(
			vec![2342u16],
			arr.iter_unmasked(..).map(|x| { *x }).collect::<Vec<_>>()
		);
		arr.write_clone(2, (&[2u16, 3u16, 4u16][..]).iter());
		assert_eq!(
			vec![2, 3, 4, 2342u16],
			arr.iter_unmasked(..).map(|x| { *x }).collect::<Vec<_>>()
		);
	}

	#[test]
	fn test_iter_filled() {
		let mut arr = MaskedArray::masked_with_value(4, 2342u16);
		{
			let mut iter = arr.iter_filled(.., &0xbeefu16);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert!(iter.next().is_none());
			assert!(iter.next().is_none());
		}
		arr.unmask(1..3);
		{
			let mut iter = arr.iter_filled(.., &0xbeefu16);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert_eq!(*iter.next().unwrap(), 2342u16);
			assert_eq!(*iter.next().unwrap(), 2342u16);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert!(iter.next().is_none());
			assert!(iter.next().is_none());
		}
		arr.mask(2..3);
		arr.unmask(3..4);
		{
			let mut iter = arr.iter_filled(.., &0xbeefu16);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert_eq!(*iter.next().unwrap(), 2342u16);
			assert_eq!(*iter.next().unwrap(), 0xbeef);
			assert_eq!(*iter.next().unwrap(), 2342u16);
			assert!(iter.next().is_none());
			assert!(iter.next().is_none());
		}
		arr.unmask(..);
		{
			let v1: Vec<_> = arr.iter_filled(.., &0xbeefu16).map(|x| *x).collect();
			let v2: Vec<_> = arr.iter().map(|x| *x).collect();
			assert_eq!(v1, v2);
		}
	}

	#[test]
	fn test_iter_unmasked_enumerated() {
		let mut arr = MaskedArray::masked_with_value(16, 2342u16);
		{
			let mut iter = arr.iter_unmasked_enumerated();
			assert!(iter.next().is_none());
		}
		arr.unmask(10..11);
		{
			let mut iter = arr.iter_unmasked_enumerated();
			assert!(iter.next().unwrap() == (10, &2342u16));
			assert!(iter.next().is_none());
		}
		arr.write_clone(2, (&[5u16, 6u16, 7u16][..]).iter());
		{
			let mut iter = arr.iter_unmasked_enumerated();
			assert!(iter.next().unwrap() == (2, &5u16));
			assert!(iter.next().unwrap() == (3, &6u16));
			assert!(iter.next().unwrap() == (4, &7u16));
			assert!(iter.next().unwrap() == (10, &2342u16));
			assert!(iter.next().is_none());
		}
	}

	#[test]
	fn test_iter_unmasked_chunks_completely_unmasked() {
		let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15u8];
		let arr: MaskedArray<_> = data.clone().into();
		for (chiter, chunk) in arr.unmasked_chunks(4).zip(data.chunks(4)) {
			let from_marr: Vec<_> = chiter.map(|x| *x).collect();
			assert_eq!(&from_marr[..], &chunk[..]);
		}
	}

	#[test]
	fn test_iter_unmasked_chunks_with_holes() {
		let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15u8];
		let mut arr: MaskedArray<_> = data.clone().into();
		arr.mask(1..3);
		arr.mask(8..12);
		let chunks: Vec<Vec<u8>> = arr
			.unmasked_chunks(4)
			.map(|x| x.map(|y| *y).collect::<Vec<_>>())
			.collect();
		assert_eq!(chunks[0], vec![1, 4u8]);
		assert_eq!(chunks[1], vec![5, 6, 7, 8u8]);
		assert_eq!(chunks[2], Vec::<u8>::new());
		assert_eq!(chunks[3], vec![13, 14, 15u8]);
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
		assert_eq!(arr[0 * 4 + 2], 10);
		assert_eq!(arr[0 * 4 + 3], 11);
		assert_eq!(arr[0 * 4 + 4], 12);
		assert_eq!(arr[0 * 4 + 5], 13);
		assert_eq!(arr[1 * 4 + 2], 10);
		assert_eq!(arr[1 * 4 + 3], 11);
		assert_eq!(arr[1 * 4 + 4], 12);
		assert_eq!(arr[1 * 4 + 5], 13);
		assert_eq!(arr[2 * 4 + 2], 10);
		assert_eq!(arr[2 * 4 + 3], 11);
		assert_eq!(arr[2 * 4 + 4], 12);
		assert_eq!(arr[2 * 4 + 5], 13);
		assert_eq!(arr[3 * 4 + 2], 10);
		assert_eq!(arr[3 * 4 + 3], 11);
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
		assert_eq!(arr[0 * 4 + 2], 10);
		assert_eq!(arr[0 * 4 + 3], 11);
		assert_eq!(arr[0 * 4 + 4], 12);
		assert_eq!(arr[0 * 4 + 5], 13);
		assert_eq!(arr[1 * 4 + 2], 10);
		assert_eq!(arr[1 * 4 + 3], 11);
		assert_eq!(arr[1 * 4 + 4], 12);
		assert_eq!(arr[1 * 4 + 5], 13);
		assert_eq!(arr[2 * 4 + 2], 10);
		assert_eq!(arr[2 * 4 + 3], 11);
		assert_eq!(arr[2 * 4 + 4], 12);
		assert_eq!(arr[2 * 4 + 5], 13);
		assert_eq!(arr[3 * 4 + 2], 10);
		assert_eq!(arr[3 * 4 + 3], 11);
		assert!(arr.get_mask().all());
	}
}
