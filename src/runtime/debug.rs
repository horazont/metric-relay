use log::{debug, warn};

use std::sync::Arc;

use smartstring::alias::{String as SmartString};

use tokio::sync::broadcast;
use tokio::sync::mpsc;

use core::time::Duration;

use chrono::Utc;

use rand;
use rand::Rng;

use crate::metric;

use super::traits;
use super::payload;
use super::adapter::Serializer;

pub struct DebugStdoutSink {
	samples: Serializer<payload::Sample>,
	stream: Serializer<payload::Stream>,
}

impl DebugStdoutSink {
	pub fn new() -> DebugStdoutSink {
		let (samples, samples_src) = Serializer::new(8);
		let (stream, stream_src) = Serializer::new(8);
		let result = DebugStdoutSink{
			samples,
			stream,
		};
		tokio::spawn(async move {
			Self::process(samples_src, stream_src).await;
			debug!("DebugStdoutSink terminating");
		});
		result
	}

	async fn process(
			mut samples: mpsc::Receiver<payload::Sample>,
			mut stream: mpsc::Receiver<payload::Stream>)
	{
		loop {
			tokio::select! {
				sample = samples.recv() => match sample {
					Some(readout) => {
						println!("  {}", readout.timestamp);
						println!("    {} @ {}", readout.path.device_type, readout.path.instance);
						for (comp, value) in readout.components.iter() {
							println!("      {} = {} {}", comp, value.magnitude, value.unit);
						}
					},
					None => {
						debug!("sample source closed, exiting");
						return
					},
				},
				stream_block = stream.recv() => match stream_block {
					Some(_) => {
						println!("stream block");
					},
					None => {
						debug!("stream source closed, exiting");
						return
					}
				},
			}
		}
	}
}

impl traits::Sink for DebugStdoutSink {
	fn attach_source<'x>(&self, src: &'x dyn traits::Source) {
		debug!("connecting debug sink");
		self.samples.attach(src.subscribe_to_samples());
		self.stream.attach(src.subscribe_to_streams());
	}
}

pub struct RandomComponent {
	pub unit: metric::Unit,
	pub min: f64,
	pub max: f64,
}

pub struct RandomSource {
	sink: broadcast::Sender<payload::Sample>,
}

impl RandomSource {
	pub fn new(interval: Duration, instance: SmartString, device_type: SmartString, components: metric::OrderedVec<SmartString, RandomComponent>) -> Self {
		let (sink, _) = broadcast::channel(8);
		let result = Self{
			sink,
		};
		result.spawn_into_background(interval, instance, device_type, components);
		result
	}

	fn spawn_into_background(&self, interval: Duration, instance: SmartString, device_type: SmartString, components: metric::OrderedVec<SmartString, RandomComponent>) {
		let sink = self.sink.clone();
		tokio::spawn(async move {
			loop {
				let timestamp = Utc::now();
				let mut result = metric::Readout{
					timestamp,
					path: metric::DevicePath{
						instance: instance.clone(),
						device_type: device_type.clone(),
					},
					components: metric::OrderedVec::new(),
				};
				{
					let mut rng = rand::thread_rng();
					for (k, v) in components.iter() {
						result.components.insert(k.clone(), metric::Value{
							unit: v.unit.clone(),
							magnitude: rng.gen::<f64>() * (v.max - v.min) + v.min,
						});
					}
				}
				match sink.send(Arc::new(result)) {
					Ok(_) => (),
					Err(_) => {
						warn!("random sample lost, no receivers");
						continue;
					}
				}
				tokio::time::sleep(interval).await;
			}
		});
	}
}

impl traits::Source for RandomSource {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sink.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		traits::null_receiver()
	}
}
