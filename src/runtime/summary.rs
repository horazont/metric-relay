use std::sync::Arc;

use log::{debug, warn};

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

use chrono::{DateTime, Duration, Utc};

use crate::metric;

use super::adapter::Serializer;
use super::payload;
use super::traits::{null_receiver, Sink, Source};

struct SummaryWorker {
	size: usize,
	source: mpsc::Receiver<payload::Stream>,
	sink: broadcast::Sender<payload::Sample>,
}

impl SummaryWorker {
	pub fn spawn(
		size: usize,
		source: mpsc::Receiver<payload::Stream>,
		sink: broadcast::Sender<payload::Sample>,
	) {
		let mut worker = Self { size, source, sink };
		tokio::spawn(async move { worker.run().await });
	}

	fn process_chunk<X: Copy>(
		t: DateTime<Utc>,
		path: &metric::DevicePath,
		chunk: &[&X],
		scale: &metric::Value,
		clip: f64,
	) -> payload::Readout
	where
		f64: From<X>,
	{
		let mut sum = 0.0f64;
		let mut min = 1.0f64 / 0.0;
		let mut max = -1.0f64 / 0.0;
		let mut sq_sum = 0.0f64;
		for v in chunk {
			let vf = (<f64 as From<X>>::from(**v)) / clip * scale.magnitude;
			sum += vf;
			sq_sum += vf * vf;
			min = min.min(vf);
			max = max.max(vf);
		}
		let lenf = chunk.len() as f64;
		let sq_avg = sq_sum / lenf;
		let rms = sq_avg.sqrt();
		let avg = sum / lenf;
		let variance = sq_avg - avg * avg;
		let stddev = variance.sqrt();

		let mut components = metric::OrderedVec::with_capacity(5);
		components.insert(
			"min".into(),
			metric::Value {
				unit: scale.unit.clone(),
				magnitude: min,
			},
		);
		components.insert(
			"max".into(),
			metric::Value {
				unit: scale.unit.clone(),
				magnitude: max,
			},
		);
		components.insert(
			"avg".into(),
			metric::Value {
				unit: scale.unit.clone(),
				magnitude: avg,
			},
		);
		components.insert(
			"rms".into(),
			metric::Value {
				unit: scale.unit.clone(),
				magnitude: rms,
			},
		);
		components.insert(
			"stddev".into(),
			metric::Value {
				unit: scale.unit.clone(),
				magnitude: stddev,
			},
		);

		Arc::new(metric::Readout {
			timestamp: t,
			path: path.clone(),
			components,
		})
	}

	fn process(size: usize, block: payload::Stream, sink: broadcast::Sender<payload::Sample>) {
		let period = Duration::from_std(block.period).unwrap();
		let mut readouts = Vec::new();

		match *block.data {
			metric::RawData::I16(ref vec) => {
				readouts.reserve((vec.len() + size - 1) / size);
				let mut buffer: Vec<&i16> = Vec::with_capacity(size);
				for (i, chunk) in vec.unmasked_chunks(size).enumerate() {
					let t = block.t0 + (period * (i * size) as i32);
					buffer.extend(chunk);
					if buffer.len() > 0 {
						readouts.push(Self::process_chunk(
							t,
							&block.path,
							&buffer[..],
							&block.scale,
							i16::MAX as f64,
						));
					}
				}
			}
			metric::RawData::F64(ref vec) => {
				readouts.reserve((vec.len() + size - 1) / size);
				let mut buffer: Vec<&f64> = Vec::with_capacity(size);
				for (i, chunk) in vec.unmasked_chunks(size).enumerate() {
					let t = block.t0 + (period * (i * size) as i32);
					buffer.extend(chunk);
					if buffer.len() > 0 {
						readouts.push(Self::process_chunk(
							t,
							&block.path,
							&buffer[..],
							&block.scale,
							1.0,
						));
					}
				}
			}
		}

		if readouts.len() == 0 {
			return;
		}

		match sink.send(readouts) {
			Ok(_) => (),
			Err(_) => {
				warn!("no receivers, summary sample lost");
			}
		}
	}

	async fn run(&mut self) {
		loop {
			let block = match self.source.recv().await {
				Some(v) => v,
				None => {
					debug!("SummaryWorker shutting down");
					return;
				}
			};

			let size = self.size;
			let sink = self.sink.clone();
			let result = spawn_blocking(move || Self::process(size, block, sink)).await;
			match result {
				Ok(_) => (),
				Err(e) => {
					warn!("summary task crashed: {}. data lost.", e);
					continue;
				}
			}
		}
	}
}

pub struct Summary {
	serializer: Serializer<payload::Stream>,
	zygote: broadcast::Sender<payload::Sample>,
}

impl Summary {
	pub fn new(size: usize) -> Self {
		let (zygote, _) = broadcast::channel(128);
		let (serializer, source) = Serializer::new(8);
		SummaryWorker::spawn(size, source, zygote.clone());
		Self { serializer, zygote }
	}
}

impl Source for Summary {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}

impl Sink for Summary {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.serializer.attach(src.subscribe_to_streams())
	}
}
