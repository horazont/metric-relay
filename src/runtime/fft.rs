use std::fmt::Write;
use std::ops::Range;
use std::sync::Arc;

use smartstring::alias::String as SmartString;

use log::{debug, trace, warn};

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

use num_traits::Zero;

use rustfft::{num_complex::Complex, Fft as FftImpl, FftPlanner};

use crate::metric;
use crate::metric::MaskedArray;

use super::adapter::Serializer;
use super::payload;
use super::traits::{null_receiver, Sink, Source};

struct FftWorker {
	inner: Arc<dyn FftImpl<f32>>,
	source: mpsc::Receiver<payload::Stream>,
	sink: broadcast::Sender<payload::Sample>,
}

fn masked_re_avg(r: Range<usize>, ma: &MaskedArray<Complex<f32>>) -> Option<f32> {
	let mut nvalid = 0usize;
	let mut sum = 0f32;
	for v in ma.iter_unmasked(r) {
		nvalid += 1;
		sum += v.re;
	}
	if nvalid == 0 {
		None
	} else {
		Some(sum / nvalid as f32)
	}
}

impl FftWorker {
	pub fn spawn(
		inner: Arc<dyn FftImpl<f32>>,
		source: mpsc::Receiver<payload::Stream>,
		sink: broadcast::Sender<payload::Sample>,
	) {
		let mut worker = FftWorker {
			inner,
			source,
			sink,
		};
		tokio::spawn(async move {
			worker.run().await;
		});
	}

	fn process(batch: payload::Stream, fft: Arc<dyn FftImpl<f32>>) -> Vec<(usize, Vec<f32>)> {
		let samples: MaskedArray<Complex<f32>> = match *batch.data {
			metric::RawData::I16(ref v) => v.with_data(
				v.iter()
					.map(|x| Complex {
						re: *x as f32 / i16::MAX as f32,
						im: 0.0,
					})
					.collect(),
			),
		};
		let mut offset = 0usize;

		let mut scratchspace = Vec::new();
		scratchspace.resize(fft.get_inplace_scratch_len(), Complex::zero());
		let mut data = Vec::with_capacity(fft.len());
		let mut result = Vec::new();
		let scale = fft.len() as f32 / 2.0;
		while offset < samples.len() {
			let r = offset..(offset + fft.len());
			if r.end > samples.len() {
				break;
			}
			data.clear();

			if !samples.get_mask()[r.clone()].all() {
				// not all values are valid, we have to fill the remaining ones. we use the average, because that will at least avoid any stray DC component. it will still cause havoc in the higher frequencies though
				let avg = match masked_re_avg(r.clone(), &samples) {
					Some(v) => v,
					// *no* value is valid in this chunk, skip it altogether
					None => {
						trace!(
							"dropping {} masked samples at {}/{}",
							fft.len(),
							offset,
							samples.len()
						);
						offset += fft.len();
						continue;
					}
				};
				trace!(
					"filled masked values of {} samples with avg {} at {}/{}",
					fft.len(),
					avg,
					offset,
					samples.len()
				);
				data.extend(samples.iter_filled(r.clone(), &Complex { re: avg, im: 0. }));
			} else {
				trace!(
					"using {} samples as-is at {}/{}",
					fft.len(),
					offset,
					samples.len()
				);
				data.extend(samples[r].iter());
			}

			fft.process_with_scratch(&mut data, &mut scratchspace);
			let npack = data.len() / 2;
			let mut pack: Vec<_> = data.drain(..=npack).map(|x| x.norm() / scale).collect();
			pack[0] /= 2.0;
			pack[npack] /= 2.0;
			result.push((offset, pack));
			offset += fft.len();
		}

		if let Some(dropped) =
			samples
				.len()
				.checked_sub(offset)
				.and_then(|x| if x > 0 { Some(x) } else { None })
		{
			warn!("fft dropped {} samples; please ensure that fft size is a multiple of the inbound stream block size", dropped);
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
				}
			};

			let mut result = {
				let fft = self.inner.clone();
				let batch = batch.clone();
				trace!(
					"spawning processor for batch with {} samples / fft size {}",
					batch.data.len(),
					fft.len()
				);
				match spawn_blocking(move || Self::process(batch, fft)).await {
					Ok(v) => v,
					Err(e) => {
						warn!("failed to FFT stream block: {}", e);
						continue;
					}
				}
			};

			let window_offset = (self.inner.len() / 2) as i64;
			let mut readouts = Vec::with_capacity(result.len());
			for (offset, mut freq_magnitudes) in result.drain(..) {
				let at = (offset as i64 + window_offset) as i32;
				let tc = batch.t0 + chrono::Duration::from_std(batch.period).unwrap() * at;
				let mut components = metric::OrderedVec::new();
				let max_freq = 1.0e9 / (batch.period.as_nanos() as f32) / 2.0;
				let nbins = freq_magnitudes.len() - 1;
				let freq_scale = max_freq / (nbins as f32);
				for (i, magnitude) in freq_magnitudes.drain(..).enumerate() {
					let freq = (i as f32) * freq_scale;
					let mut buf = SmartString::new();
					write!(&mut buf, "{}", freq).unwrap();
					components.insert(
						buf,
						metric::Value {
							magnitude: magnitude as f64,
							unit: batch.scale.unit.clone(),
						},
					);
				}

				readouts.push(Arc::new(metric::Readout {
					timestamp: tc,
					path: batch.path.clone(),
					components,
				}));
			}
			match self.sink.send(readouts) {
				Ok(_) => (),
				Err(_) => {
					warn!("lost fft processed sample, no receivers");
				}
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
		FftWorker::spawn(fft, source, zygote.clone());
		Self { serializer, zygote }
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
