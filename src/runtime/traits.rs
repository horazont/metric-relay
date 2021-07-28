use tokio::sync::broadcast;

use super::payload;

pub trait Source {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample>;
	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream>;
}

pub trait Sink {
	fn attach_source<'x>(&mut self, src: &'x dyn Source);
}

pub fn null_receiver<T: Clone>() -> broadcast::Receiver<T> {
	let (_, receiver)= broadcast::channel(1);
	receiver
}
