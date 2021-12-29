use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::ops::Deref;

use super::result::EvalResult;

pub trait Namespace {
	fn lookup<'x>(&self, name: &'x str) -> Option<f64>;
}

impl<K: Borrow<str> + Hash + Eq> Namespace for HashMap<K, f64> {
	fn lookup<'x>(&self, name: &'x str) -> Option<f64> {
		self.get(name).and_then(|v| Some(*v))
	}
}

pub enum BoxCow<'x, T: ?Sized + 'x> {
	Owned(Box<T>),
	Borrowed(&'x T),
}

impl<'x, T: ?Sized + 'x> BoxCow<'x, T> {
	pub fn wrap_ref(r: &'x T) -> Self {
		Self::Borrowed(r)
	}
}

impl<'x, T: ?Sized + 'x> Deref for BoxCow<'x, T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		match self {
			Self::Owned(ref b) => b.deref(),
			Self::Borrowed(r) => r,
		}
	}
}

impl<'x, T: ?Sized + 'x> From<&'x T> for BoxCow<'x, T> {
	fn from(r: &'x T) -> Self {
		Self::Borrowed(r)
	}
}

impl<'x, T: ?Sized + 'x> From<Box<T>> for BoxCow<'x, T> {
	fn from(r: Box<T>) -> Self {
		Self::Owned(r)
	}
}

pub struct Context<'x> {
	ns: BoxCow<'x, dyn Namespace>,
}

impl<'x> Context<'x> {
	pub fn empty() -> Context<'static> {
		Context {
			ns: BoxCow::Owned(Box::new(HashMap::<&'static str, f64>::new())),
		}
	}

	pub fn new<T: Into<BoxCow<'x, (dyn Namespace + 'x)>>>(ns: T) -> Self {
		Self { ns: ns.into() }
	}

	pub fn lookup<T: Borrow<str>>(&self, name: T) -> Option<f64> {
		self.ns.lookup(name.borrow())
	}
}

pub trait Evaluate: fmt::Debug + fmt::Display + Send + Sync {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64>;
}
