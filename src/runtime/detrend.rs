use std::sync::Arc;

use log::{warn, debug};

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

use crate::metric;

use super::adapter::Serializer;
use super::payload;
use super::traits::{Source, Sink, null_receiver};


#[derive(Debug, Clone, Copy)]
pub enum Mode {
	Constant,
	Linear,
}

struct DetrendWorker {
	source: mpsc::Receiver<payload::Stream>,
	sink: broadcast::Sender<payload::Stream>,
	mode: Mode,
}

fn linear_regression<'a>(vs: impl Iterator<Item = &'a f32>) -> (f32, f32) {
	let mut prodsum = 0f32;
	let mut xsum = 0f32;
	let mut ysum = 0f32;
	let mut xsqsum = 0f32;
	let mut n = 0;

	for y in vs {
		let x = n as f32;
		n += 1;
		prodsum += x*y;
		xsum += x;
		xsqsum += x*x;
		ysum += y;
	}

	let n = n as f32;
	let beta = (n * prodsum - xsum*ysum) / (n * xsqsum - xsum*xsum);
	let alpha = ysum / n - beta * xsum / n;
	(alpha, beta)
}

fn debias<'x>(ys: &'x mut [f32]) {
	let mut ysum = 0f32;
	let mut n = 0usize;
	for y in ys.iter() {
		ysum += *y;
		n += 1;
	}

	let n = n as f32;
	let avg = ysum / n;
	for y in ys.iter_mut() {
		*y = *y - avg;
	}
}

fn detrend<'x>(ys: &'x mut [f32]) {
	let (alpha, beta) = linear_regression(ys.iter());
	for (x, y) in ys.iter_mut().enumerate() {
		let x = x as f32;
		*y = *y - (alpha + x * beta);
	}
}

impl DetrendWorker {
	pub fn spawn(
			source: mpsc::Receiver<payload::Stream>,
			sink: broadcast::Sender<payload::Stream>,
			mode: Mode,
			)
	{
		let mut worker = Self{
			source,
			sink,
			mode,
		};
		tokio::spawn(async move {
			worker.run().await
		});
	}

	fn process(
			block: payload::Stream,
			sink: broadcast::Sender<payload::Stream>,
			mode: Mode)
	{
		// TODO: take masking into account then
		let mut floats: Vec<_> = match block.data {
			metric::RawData::I16(ref vs) => {
				vs.iter().map(|x| { *x as f32 }).collect()
			},
		};
		match mode {
			Mode::Constant => debias(&mut floats[..]),
			Mode::Linear => detrend(&mut floats[..]),
		};
		let result = Arc::new(metric::StreamBlock{
			t0: block.t0,
			seq0: block.seq0,
			period: block.period,
			path: block.path.clone(),
			scale: block.scale.clone(),
			data: match block.data {
				metric::RawData::I16(_) => {
					metric::RawData::I16(
						floats.iter().map(|x| {
							x.min(i16::MAX as f32).max(i16::MIN as f32) as i16
						}).collect()
					)
				},
			},
		});
		match sink.send(result) {
			Ok(_) => (),
			Err(_) => warn!("lost detrended stream block because no receivers were ready"),
		}
	}

	async fn run(&mut self) {
		loop {
			let block = match self.source.recv().await {
				Some(v) => v,
				None => {
					debug!("DetrendWorker shutting down");
					return;
				}
			};

			let sink = self.sink.clone();
			let mode = self.mode;
			let result = spawn_blocking(move || {
				Self::process(block, sink, mode)
			}).await;
			match result {
				Ok(_) => (),
				Err(e) => {
					warn!("detrend task crashed: {}. data lost.", e);
					continue;
				},
			}
		}
	}
}


pub struct Detrend {
	serializer: Serializer<payload::Stream>,
	zygote: broadcast::Sender<payload::Stream>,
}

impl Detrend {
	pub fn new(mode: Mode) -> Self {
		let (zygote, _) = broadcast::channel(128);
		let (serializer, source) = Serializer::new(8);
		DetrendWorker::spawn(
			source,
			zygote.clone(),
			mode,
		);
		Self{
			serializer,
			zygote,
		}
	}
}

impl Source for Detrend {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		null_receiver()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.zygote.subscribe()
	}
}

impl Sink for Detrend {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.serializer.attach(src.subscribe_to_streams())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_linear_regression_trivial() {
		let vs = vec![0., 1., 2.];
		let (alpha, beta) = linear_regression(vs.iter());
		assert_eq!(beta, 1.);
		assert_eq!(alpha, 0.);
	}

	#[test]
	fn test_linear_regression() {
		let vs = vec![1.0, 1.5, 2.0];
		let (alpha, beta) = linear_regression(vs.iter());
		assert_eq!(beta, 0.5);
		assert_eq!(alpha, 1.0);
	}

	#[test]
	fn test_linear_regression_negative() {
		let vs = vec![-1.0, -1.5, -2.0];
		let (alpha, beta) = linear_regression(vs.iter());
		assert_eq!(beta, -0.5);
		assert_eq!(alpha, -1.0);
	}

	#[test]
	fn test_linear_regression_flat() {
		let vs = vec![-1.0, -1.0, -1.0];
		let (alpha, beta) = linear_regression(vs.iter());
		assert_eq!(beta, 0.);
		assert_eq!(alpha, -1.0);
	}

	#[test]
	fn test_detrend_flat() {
		let mut vs = vec![-1.0, -1.0, -1.0];
		detrend(&mut vs[..]);
		for v in vs {
			assert_eq!(v, 0.);
		}
	}

	#[test]
	fn test_detrend_linear() {
		let mut vs = vec![-1.0, 0.0, 1.0];
		detrend(&mut vs[..]);
		for v in vs {
			assert_eq!(v, 0.);
		}
	}

	#[test]
	#[ignore]
	fn test_detrend_sine() {
		let mut vs = vec![0., 1., 0., -1.];
		let base = vs.clone();
		detrend(&mut vs[..]);
		assert_eq!(vs, base);
	}

	// the detrending cannot deal with periodics nicely
	#[test]
	#[ignore]
	fn test_detrend_sine_with_linear_trend() {
		let base = vec![0., 1., 0., -1.];
		let mut vs = vec![0., 1.5, 1.0, 0.5];
		detrend(&mut vs[..]);
		assert_eq!(vs, base);
	}

	#[test]
	fn test_debias_flat() {
		let mut vs = vec![-1.0, -1.0, -1.0];
		debias(&mut vs[..]);
		for v in vs {
			assert_eq!(v, 0.);
		}
	}

	#[test]
	fn test_debias_linear() {
		let mut vs = vec![0.0, 1.0, 2.0];
		let reference = vec![-1.0, 0.0, 1.0];
		debias(&mut vs[..]);
		assert_eq!(vs, reference);
	}

	#[test]
	fn test_debias_sine() {
		let mut vs = vec![0., 1., 0., -1.];
		let base = vs.clone();
		debias(&mut vs[..]);
		assert_eq!(vs, base);
	}

	#[test]
	fn test_debias_sine_with_constant_offset() {
		let base = vec![0., 1., 0., -1.];
		let mut vs = vec![0.5, 1.5, 0.5, -0.5];
		debias(&mut vs[..]);
		assert_eq!(vs, base);
	}
}
