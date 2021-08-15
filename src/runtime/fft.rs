use std::fmt::Write;
use std::sync::Arc;

use smartstring::alias::{String as SmartString};

use log::{debug, warn};

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

use num_traits::Zero;

use rustfft::{FftPlanner, num_complex::Complex, Fft as FftImpl};

use crate::metric;

use super::payload;
use super::adapter::Serializer;
use super::traits::{Source, Sink, null_receiver};

struct FftWorker {
	inner: Arc<dyn FftImpl<f32>>,
	source: mpsc::Receiver<payload::Stream>,
	sink: broadcast::Sender<payload::Sample>,
}

impl FftWorker {
	pub fn spawn(inner: Arc<dyn FftImpl<f32>>, source: mpsc::Receiver<payload::Stream>, sink: broadcast::Sender<payload::Sample>) {
		let mut worker = FftWorker{
			inner,
			source,
			sink,
		};
		tokio::spawn(async move {
			worker.run().await;
		});
	}

	fn process(batch: payload::Stream, fft: Arc<dyn FftImpl<f32>>) -> Vec<(i64, Vec<f32>)> {
		let mut samples: Vec<_> = match *batch.data {
			metric::RawData::I16(ref v) => v.iter().map(|x| { Complex{re: *x as f32 / i16::MAX as f32, im: 0.0} }).collect(),
		};
		let mut offset = 0;

		let mut scratchspace = Vec::new();
		scratchspace.resize(fft.get_inplace_scratch_len(), Complex::zero());
		let mut data = Vec::with_capacity(fft.len());
		let mut result = Vec::new();
		let scale = fft.len() as f32 / 2.0;
		while samples.len() >= fft.len() {
			data.clear();
			data.extend(samples.drain(0..fft.len()));
			fft.process_with_scratch(
				&mut data,
				&mut scratchspace,
			);
			let npack = data.len() / 2;
			let mut pack: Vec<_> = data.drain(..=npack).map(|x| { x.norm() / scale}).collect();
			pack[0] /= 2.0;
			pack[npack] /= 2.0;
			result.push((offset, pack));
			offset += fft.len() as i64;
		}
		if samples.len() > 0 {
			warn!("fft dropped {} samples; please ensure that fft size is a multiple of the inbound stream block size", samples.len());
		}

		result
	}

	async fn run(&mut self) {
		loop {
			let batch = match self.source.recv().await {
				Some(v) => v,
				None => {
					debug!("shutting down fft worker, source is gone");
					return;
				},
			};

			let mut result = {
				let fft = self.inner.clone();
				let batch = batch.clone();
				match spawn_blocking(move || { Self::process(batch, fft) }).await {
					Ok(v) => v,
					Err(e) => {
						warn!("failed to FFT stream block: {}", e);
						continue;
					},
				}
			};

			let window_offset = (self.inner.len() / 2) as i64;
			let mut readouts = Vec::with_capacity(result.len());
			for (offset, mut freq_magnitudes) in result.drain(..) {
				let at = (offset + window_offset) as i32;
				let tc = batch.t0 + chrono::Duration::from_std(batch.period).unwrap() * at;
				let mut components = metric::OrderedVec::new();
				let max_freq = 1.0e9 / (batch.period.as_nanos() as f32) / 2.0;
				let nbins = freq_magnitudes.len() - 1;
				let freq_scale = max_freq / (nbins as f32);
				for (i, magnitude) in freq_magnitudes.drain(..).enumerate() {
					let freq = (i as f32) * freq_scale;
					let mut buf = SmartString::new();
					write!(&mut buf, "{}", freq).unwrap();
					components.insert(buf, metric::Value{magnitude: magnitude as f64, unit: batch.scale.unit.clone()});
				};

				readouts.push(Arc::new(metric::Readout{
					timestamp: tc,
					path: batch.path.clone(),
					components,
				}));

			}
			match self.sink.send(readouts) {
				Ok(_) => (),
				Err(_) => {
					warn!("lost fft processed sample, no receivers");
				},
			}
		}
	}
}

pub struct Fft {
	serializer: Serializer<payload::Stream>,
	zygote: broadcast::Sender<payload::Sample>,
}

impl Fft {
	pub fn new(size: usize) -> Self {
		let (zygote, _) = broadcast::channel(128);
		let (serializer, source) = Serializer::new(8);
		let fft = FftPlanner::new().plan_fft_forward(size);
		FftWorker::spawn(
			fft,
			source,
			zygote.clone(),
		);
		Self{
			serializer,
			zygote,
		}
	}
}

impl Source for Fft {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}

impl Sink for Fft {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.serializer.attach(src.subscribe_to_streams())
	}
}
