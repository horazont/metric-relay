use std::borrow::Borrow;
use std::cmp::Ord;
use std::fmt;
use std::ops::RangeBounds;

use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct OrderedVec<K, V>(Vec<(K, V)>);

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for OrderedVec<K, V> {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		let mut dbg = f.debug_map();
		for (ref k, ref v) in self.0.iter() {
			dbg.entry(k, v);
		}
		dbg.finish()
	}
}

impl<K: Clone, V: Clone> Clone for OrderedVec<K, V> {
	fn clone(&self) -> Self {
		OrderedVec(self.0.clone())
	}
}

impl<K: PartialEq, V: PartialEq> PartialEq for OrderedVec<K, V> {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

impl<K, V> OrderedVec<K, V> {
	pub fn new() -> Self {
		Self(Vec::new())
	}

	pub fn with_capacity(capacity: usize) -> Self {
		Self(Vec::with_capacity(capacity))
	}

	#[inline]
	fn find<Q: ?Sized>(&self, k: &Q) -> Result<usize, usize>
		where
			K: Borrow<Q>,
			Q: Ord
	{
		self.0.binary_search_by(|kv: &(K, V)| { Ord::cmp(kv.0.borrow(), &k) })
	}

	pub fn insert(&mut self, k: K, mut v: V) -> Option<V>
		where K: Ord
	{
		match self.0.binary_search_by(|kv: &(K, V)| { Ord::cmp(&kv.0, &k) }) {
			Ok(existing) => {
				std::mem::swap(&mut v, &mut self.0[existing].1);
				Some(v)
			}
			Err(target) => {
				self.0.insert(target, (k, v));
				None
			},
		}
	}

	pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
		where
			K: Borrow<Q>,
			Q: Ord
	{
		match self.find(k) {
			Ok(existing) => Some(&self.0[existing].1),
			Err(_) => None,
		}
	}

	pub fn get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
		where
			K: Borrow<Q>,
			Q: Ord
	{
		match self.find(k) {
			Ok(existing) => Some(&mut self.0[existing].1),
			Err(_) => None,
		}
	}

	pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
		where
			K: Borrow<Q>,
			Q: Ord
	{
		match self.find(k) {
			Ok(_) => true,
			Err(_) => false,
		}
	}

	pub fn remove_entry<Q: ?Sized>(&mut self, k: &Q) -> Option<(K, V)>
		where
			K: Borrow<Q>,
			Q: Ord
	{
		match self.find(k) {
			Ok(existing) => Some(self.0.remove(existing)),
			Err(_) => None,
		}
	}

	pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<V>
		where
			K: Borrow<Q>,
			Q: Ord
	{
		self.remove_entry(k).and_then(|kv| { Some(kv.1) })
	}

	pub fn len(&self) -> usize {
		self.0.len()
	}

	pub fn capacity(&self) -> usize {
		self.0.capacity()
	}

	pub fn shrink_to_fit(&mut self) {
		self.0.shrink_to_fit()
	}

	pub fn reserve(&mut self, additional: usize) {
		self.0.reserve(additional)
	}

	pub fn reserve_exact(&mut self, additional: usize) {
		self.0.reserve_exact(additional)
	}

	pub fn drain(&mut self, range: impl RangeBounds<usize>) -> std::vec::Drain<'_, (K, V)> {
		self.0.drain(range)
	}

	pub fn retain<F: FnMut(&(K, V)) -> bool>(&mut self, f: F) {
		self.0.retain(f)
	}
}

impl<K, V> From<OrderedVec<K, V>> for Vec<(K, V)> {
	fn from(other: OrderedVec<K, V>) -> Self {
		other.0
	}
}

impl<K, V> std::ops::Deref for OrderedVec<K, V> {
	type Target = [(K, V)];

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_insert_and_get() {
		let mut v = OrderedVec::new();
		v.insert("foobar", 23.42f32);
		assert_eq!(*v.get("foobar").unwrap(), 23.42f32);
	}

	#[test]
	fn test_insert_returns_old_value() {
		let mut v = OrderedVec::new();
		v.insert("k1", "foo");
		assert_eq!(v.insert("k1", "bar").unwrap(), "foo");
	}

	#[test]
	fn test_insert_multiple() {
		let mut v = OrderedVec::new();
		assert!(v.insert("k1", "foo").is_none());
		assert!(v.insert("k2", "foo").is_none());
		assert!(v.insert("k3", "foo").is_none());
	}

	#[test]
	fn test_contains_key() {
		let mut v = OrderedVec::new();
		assert!(!v.contains_key("k1"));
		assert!(!v.contains_key("k2"));
		assert!(!v.contains_key("k3"));

		assert!(v.insert("k1", "foo").is_none());
		assert!(v.contains_key("k1"));
		assert!(!v.contains_key("k2"));
		assert!(!v.contains_key("k3"));

		assert!(v.insert("k2", "foo").is_none());
		assert!(v.contains_key("k1"));
		assert!(v.contains_key("k2"));
		assert!(!v.contains_key("k3"));

		assert!(v.insert("k3", "foo").is_none());
		assert!(v.contains_key("k1"));
		assert!(v.contains_key("k2"));
		assert!(v.contains_key("k3"));
	}

	#[test]
	fn test_get_returns_none_on_nonexistant_key() {
		let v = OrderedVec::<&'static str, i32>::new();
		assert!(v.get("foobar").is_none());
	}

	#[test]
	fn test_get_mut() {
		let mut v = OrderedVec::<&'static str, i32>::new();
		v.insert("foobar", 10);
		*v.get_mut("foobar").unwrap() = 20;
		assert_eq!(*v.get("foobar").unwrap(), 20);
	}

	#[test]
	fn test_remove_entry() {
		let mut v = OrderedVec::new();
		v.insert("k1", "foo");
		v.insert("k2", "bar");
		v.insert("k3", "baz");

		let e = v.remove_entry("k2").unwrap();
		assert_eq!(e.0, "k2");
		assert_eq!(e.1, "bar");

		assert!(!v.contains_key("k2"));
		assert!(v.get("k2").is_none());
	}
}
