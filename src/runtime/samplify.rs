use std::sync::Arc;

use smartstring::alias::String as SmartString;

use tokio::sync::{broadcast, mpsc};

use crate::metric::{OrderedVec, Readout, Value};

use super::adapter::Serializer;
use super::payload;
use super::traits::{null_receiver, Sink, Source};

async fn samplify(
	component: SmartString,
	mut stream_source: mpsc::Receiver<payload::Stream>,
	sample_sink: broadcast::Sender<payload::Sample>,
) {
	loop {
		let block = match stream_source.recv().await {
			Some(item) => item,
			None => return,
		};

		let mut samples = Vec::new();
		for (i, sample) in block.data.iter().enumerate() {
			let normalized = match sample.normalized() {
				Some(v) => v,
				None => continue,
			};
			let timestamp = block.t0
				+ match chrono::Duration::from_std((i as u32) * block.period) {
					Ok(v) => v,
					Err(e) => {
						log::warn!(
							"discarding stream sample: cannot calculate timestamp: {}",
							e
						);
						continue;
					}
				};
			let value = Value {
				magnitude: block.scale.magnitude * normalized,
				unit: block.scale.unit.clone(),
			};
			samples.push(Arc::new(Readout {
				timestamp,
				path: block.path.clone(),
				components: OrderedVec::single(component.clone(), value),
			}));
		}
		match sample_sink.send(samples) {
			Ok(_) => (),
			Err(_) => {
				log::warn!("no receivers, samplified block lost");
			}
		}
	}
}

pub struct Samplify {
	streams: Serializer<payload::Stream>,
	sample_zygote: broadcast::Sender<payload::Sample>,
}

impl Samplify {
	pub fn new(component: SmartString) -> Self {
		let (streams, stream_source) = Serializer::new(128);
		let (sample_zygote, _) = broadcast::channel(128);
		let sample_sink = sample_zygote.clone();
		tokio::spawn(async move { samplify(component, stream_source, sample_sink).await });
		Self {
			streams,
			sample_zygote,
		}
	}
}

impl Source for Samplify {
	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}

	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sample_zygote.subscribe()
	}
}

impl Sink for Samplify {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.streams.attach(src.subscribe_to_streams())
	}
}
