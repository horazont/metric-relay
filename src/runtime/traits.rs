use std::sync::Arc;

use tokio::sync::broadcast;

use super::payload;

pub trait Source {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample>;
	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream>;
}

pub trait Sink {
	fn attach_source<'x>(&self, src: &'x dyn Source);
}

// may be unused depending on the feature set, but encoding that usedness in a cfg flag would be insane
#[allow(dead_code)]
pub fn null_receiver<T: Clone>() -> broadcast::Receiver<T> {
	let (_, receiver)= broadcast::channel(1);
	receiver
}

pub struct Node(Option<Arc<dyn Source>>, Option<Arc<dyn Sink>>);

impl Node {
	pub fn from_source(v: impl Source + 'static) -> Self {
		Self(Some(Arc::new(v)), None)
	}

	pub fn from_sink(v: impl Sink + 'static) -> Self {
		Self(None, Some(Arc::new(v)))
	}

	pub fn from(v: impl Sink + Source + 'static) -> Self {
		let obj = Arc::new(v);
		Self(Some(obj.clone()), Some(obj))
	}

	pub fn as_sink(&self) -> Option<&dyn Sink> {
		match self.1.as_ref() {
			Some(ptr) => Some(&**ptr),
			None => None,
		}
	}

	pub fn as_source(&self) -> Option<&dyn Source> {
		match self.0.as_ref() {
			Some(ptr) => Some(&**ptr),
			None => None,
		}
	}
}
