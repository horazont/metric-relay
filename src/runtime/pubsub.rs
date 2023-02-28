use log::warn;

use tokio::sync::mpsc;

use crate::pubsub;

use super::adapter::Serializer;
use super::payload;
use super::traits;

struct PubSubWorker {
	client: pubsub::Client,
	samples: mpsc::Receiver<payload::Sample>,
}

impl PubSubWorker {
	async fn run(&mut self) {
		loop {
			let mut readouts = match self.samples.recv().await {
				None => return,
				Some(v) => v,
			};
			for readout in readouts.drain(..) {
				match self.client.post(&readout).await {
					Ok(_) => (),
					Err(e) => warn!("lost sample: failed to submit to pubsub: {}", e),
				};
			}
		}
	}
}

pub struct PubSubSink {
	samples: Serializer<payload::Sample>,
}

impl PubSubSink {
	pub fn new(api_url: String, node_template: String, override_host: Option<String>) -> Self {
		let (serializer, samples) = Serializer::new(32);
		let mut worker = PubSubWorker {
			client: pubsub::Client::new(api_url, node_template, override_host),
			samples,
		};
		tokio::spawn(async move { worker.run().await });
		Self {
			samples: serializer,
		}
	}
}

impl traits::Sink for PubSubSink {
	fn attach_source<'x>(&self, src: &'x dyn traits::Source) {
		self.samples.attach(src.subscribe_to_samples())
	}
}
