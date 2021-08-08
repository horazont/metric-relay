use log::{debug, warn, error};

use std::sync::Arc;

use num_traits::float::FloatConst;

use smartstring::alias::{String as SmartString};

use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::mpsc;

use core::time::Duration;

use chrono::Utc;

use rand;
use rand::Rng;

use crate::metric;
use crate::stream;

use super::traits;
use super::payload;
use super::adapter::{Serializer, BufferedStream, BufferedStreamError};

pub struct DebugStdoutSink {
	samples: Serializer<payload::Sample>,
	stream: Serializer<payload::Stream>,
}

impl DebugStdoutSink {
	pub fn new() -> DebugStdoutSink {
		let (samples, samples_src) = Serializer::new(128);
		let (stream, stream_src) = Serializer::new(128);
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
					Some(v) => {
						println!("{:?}", v);
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

pub struct SineSourceWorker<T: stream::StreamBuffer + Send + Sync + 'static + ?Sized> {
	nsamples: u16,
	sample_period: Duration,
	path: metric::DevicePath,
	scale: metric::Value,
	period: f64,
	sink: BufferedStream<T>,
	seq: u16,
	sint: usize,
	phase_offset: f64,
	stop_ch: oneshot::Receiver<()>,
}

impl<T: stream::StreamBuffer + Send + Sync + 'static + ?Sized> SineSourceWorker<T> {
	pub fn start(nsamples: u16, sample_period: Duration, path: metric::DevicePath, scale: metric::Value, period: f64, phase: f64, buffer: Box<T>, sink: broadcast::Sender<payload::Stream>, stop_ch: oneshot::Receiver<()>) {
		let mut worker = SineSourceWorker{
			nsamples,
			sample_period,
			path,
			scale,
			period,
			sink: BufferedStream::new(
				buffer,
				sink,
			),
			seq: 0,
			sint: 0,
			phase_offset: phase,
			stop_ch,
		};
		tokio::spawn(async move {
			worker.run().await;
		});
	}

	async fn run(&mut self) {
		let emit_interval = self.sample_period * self.nsamples as u32;
		loop {
			let t0 = Utc::now();
			let seq0 = self.seq;
			select! {
				_ = tokio::time::sleep(emit_interval) => (),
				_ = &mut self.stop_ch => {
					debug!("SineSourceWorker stop_ch returned, shutting down");
					return
				},
			};
			self.seq = self.seq.wrapping_add(self.nsamples);
			let mut buf = Vec::new();
			buf.reserve(self.nsamples as usize);
			for _i in 0..self.nsamples {
				let t = ((self.sint as f64) / self.period + self.phase_offset) * f64::PI() * 2.0;
				let vf = t.sin();
				let v = (vf * i16::MAX as f64).round() as i16;
				buf.push(v);
				self.sint = self.sint.wrapping_add(1);
			};
			match self.sink.send(Arc::new(metric::StreamBlock{
				t0,
				seq0,
				path: self.path.clone(),
				period: self.sample_period,
				scale: self.scale.clone(),
				data: metric::RawData::I16(buf),
			})) {
				Ok(_) => (),
				Err(BufferedStreamError::BufferWriteError(e)) => {
					error!("sine stream block lost: {}", e);
				},
				Err(BufferedStreamError::SendError(_)) => warn!("sine stream lost, no receivers"),
			};
		}
	}
}

pub struct SineSource {
	zygote: broadcast::Sender<payload::Stream>,
	#[allow(dead_code)]
	guard: oneshot::Sender<()>,
}

impl SineSource {
	pub fn new<T: stream::StreamBuffer + Send + Sync + 'static + ?Sized>(nsamples: u16, sample_period: Duration, path: metric::DevicePath, scale: metric::Value, period: f64, phase: f64, buffer: Box<T>) -> Self {
		let (guard, stop_ch) = oneshot::channel();
		let (zygote, _) = broadcast::channel(8);
		SineSourceWorker::start(
			nsamples,
			sample_period,
			path,
			scale,
			period,
			phase,
			buffer,
			zygote.clone(),
			stop_ch,
		);
		Self{
			zygote,
			guard,
		}
	}
}

impl traits::Source for SineSource {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		traits::null_receiver()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.zygote.subscribe()
	}
}
