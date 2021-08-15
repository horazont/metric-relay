use log::{warn};

use tokio::sync::mpsc;

use crate::stream::ArchiveWrite;

use super::adapter::Serializer;
use super::payload;
use super::traits::{Source, Sink};


struct ArchiveWorker {
	inner: Box<dyn ArchiveWrite + Send + Sync + 'static>,
	source: mpsc::Receiver<payload::Stream>,
}

impl ArchiveWorker {
	fn spawn(inner: Box<dyn ArchiveWrite + Send + Sync + 'static>, source: mpsc::Receiver<payload::Stream>) {
		let mut worker = Self{
			inner,
			source,
		};
		tokio::spawn(async move {
			worker.run().await
		});
	}

	async fn run(&mut self) {
		loop {
			let block = match self.source.recv().await {
				Some(v) => v,
				None => return,
			};
			match self.inner.write(&block) {
				Ok(_) => (),
				Err(e) => {
					warn!("lost stream block: write to archive failed: {}", e);
				},
			}
		}
	}
}

pub struct Archiver {
	serializer: Serializer<payload::Stream>,
}

impl Archiver {
	pub fn new(inner: Box<dyn ArchiveWrite + Send + Sync + 'static>) -> Self {
		let (serializer, source) = Serializer::new(32);
		ArchiveWorker::spawn(inner, source);
		Self{
			serializer,
		}
	}
}

impl Sink for Archiver {
	fn attach_source<'x>(&self, source: &'x dyn Source) {
		self.serializer.attach(source.subscribe_to_streams());
	}
}
