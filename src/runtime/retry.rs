use std::io;
use std::ops::Fn;

use tokio::time::Duration;

pub struct Retry<T: ?Sized> {
	interval: Duration,
	inner: Box<T>,
}

impl<U, T: Fn() -> io::Result<U> + ?Sized> Retry<T> {
	pub fn new(inner: Box<T>, interval: Duration) -> Self {
		Self { inner, interval }
	}

	pub async fn obtain<E: Fn(io::Error) -> ()>(&self, error_handler: E) -> U {
		loop {
			match (self.inner)() {
				Ok(v) => return v,
				Err(e) => {
					error_handler(e);
					tokio::time::sleep(self.interval).await;
				}
			}
		}
	}
}
