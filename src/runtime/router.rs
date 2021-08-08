use std::sync::Arc;

use log::{trace, warn};

use tokio::sync::broadcast;
use tokio::sync::mpsc;

use super::payload;
use super::adapter::Serializer;
use super::traits::{Source, Sink, null_receiver};
use super::filter::Filter;


struct RouterWorker {
	filters: Vec<Box<dyn Filter>>,
}

impl RouterWorker {
	fn spawn(
		filters: Vec<Box<dyn Filter>>,
		sample_source: mpsc::Receiver<payload::Sample>,
		stream_source: mpsc::Receiver<payload::Stream>,
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>)
	{
		let sample_worker = Arc::new(RouterWorker{
			filters,
		});
		let stream_worker = sample_worker.clone();
		tokio::spawn(async move {
			sample_worker.run_samples(sample_source, sample_sink).await;
		});
		tokio::spawn(async move {
			stream_worker.run_streams(stream_source, stream_sink).await;
		});
	}

	async fn run_samples(
			&self,
			mut source: mpsc::Receiver<payload::Sample>,
			sink: broadcast::Sender<payload::Sample>) {
		loop {
			let mut item = match source.recv().await {
				Some(item) => item,
				// channel closed, which means that the serializer dropped, which means that the router itself was dropped, which means we can also just go home now.
				None => return,
			};
			item = match self.filters.process_readout(item) {
				Some(new) => new,
				None => {
					trace!("readout got dropped by filter");
					continue;
				},
			};
			match sink.send(item) {
				Ok(_) => (),
				Err(_) => {
					warn!("no receivers on route, dropping sample");
					continue;
				}
			}
		}
	}

	async fn run_streams(
			&self,
			mut source: mpsc::Receiver<payload::Stream>,
			sink: broadcast::Sender<payload::Stream>) {
		loop {
			let mut item = match source.recv().await {
				Some(item) => item,
				// channel closed, which means that the serializer dropped, which means that the router itself was dropped, which means we can also just go home now.
				None => return,
			};
			item = match self.filters.process_stream(item) {
				Some(new) => new,
				None => {
					trace!("stream got dropped by filter");
					continue;
				},
			};
			match sink.send(item) {
				Ok(_) => (),
				Err(_) => {
					warn!("no receivers on route, dropping stream");
					continue;
				}
			}
		}
	}
}

pub struct Router {
	samples: Serializer<payload::Sample>,
	streams: Serializer<payload::Stream>,
	sample_zygote: broadcast::Sender<payload::Sample>,
	stream_zygote: broadcast::Sender<payload::Stream>,
}

impl Router {
	pub fn new(filters: Vec<Box<dyn Filter>>) -> Self {
		let (sample_zygote, _) = broadcast::channel(128);
		let (samples, sample_source) = Serializer::new(128);
		let (stream_zygote, _) = broadcast::channel(128);
		let (streams, stream_source) = Serializer::new(128);
		RouterWorker::spawn(
			filters,
			sample_source,
			stream_source,
			sample_zygote.clone(),
			stream_zygote.clone(),
		);
		Self{
			samples,
			streams,
			sample_zygote,
			stream_zygote,
		}
	}
}

impl Source for Router {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sample_zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.stream_zygote.subscribe()
	}
}

impl Sink for Router {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.samples.attach(src.subscribe_to_samples());
		self.streams.attach(src.subscribe_to_streams());
	}
}
