use std::sync::Arc;

use log::{trace, warn};

use tokio::sync::broadcast;
use tokio::sync::mpsc;

use super::payload;
use super::adapter::Serializer;
use super::traits::{Source, Sink, null_receiver};
use super::filter::Filter;

pub struct SampleRouter {
	serializer: Serializer<payload::Sample>,
	sink: broadcast::Sender<payload::Sample>,
}

impl SampleRouter {
	pub fn new(filters: Vec<Box<dyn Filter>>) -> Self {
		let (sink, _) = broadcast::channel(128);
		let (serializer, source) = Serializer::new(8);
		let result = Self{
			serializer,
			sink,
		};
		result.spawn_into_background(source, filters);
		result
	}

	fn spawn_into_background(
			&self,
			mut source: mpsc::Receiver<payload::Sample>,
			filters: Vec<Box<dyn Filter>>) {
		let sink = self.sink.clone();
		tokio::spawn(async move {
			loop {
				let mut item = match source.recv().await {
					Some(item) => item,
					// channel closed, which means that the serializer dropped, which means that the router itself was dropped, which means we can also just go home now.
					None => return,
				};
				if filters.len() > 0 {
					let buffer = match filters.process((*item).clone()) {
						Some(new) => new,
						None => {
							trace!("readout got dropped by filter");
							continue;
						},
					};
					let raw_item = Arc::make_mut(&mut item);
					*raw_item = buffer;
				}
				match sink.send(item) {
					Ok(_) => (),
					Err(_) => {
						warn!("no receivers on route, dropping item");
						continue;
					}
				}
			}
		});
	}
}

impl Source for SampleRouter {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sink.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}

impl Sink for SampleRouter {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.serializer.attach(src.subscribe_to_samples())
	}
}
