use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use log::warn;

use chrono::{DateTime, DurationRound, Utc};

use tokio::sync::{broadcast, mpsc};

use smartstring::alias::String as SmartString;

use crate::metric;
use crate::stream;
use crate::stream::StreamBuffer;

use super::adapter::Serializer;
use super::payload;
use super::traits::{null_receiver, Sink, Source};

#[derive(Debug, Clone, Copy)]
enum SubmitError {
	TimeError(chrono::RoundingError),
	TimeMovingBackward,
	Duplicate,
	BufferError(stream::WriteError),
}

impl From<chrono::RoundingError> for SubmitError {
	fn from(other: chrono::RoundingError) -> Self {
		Self::TimeError(other)
	}
}

impl From<stream::WriteError> for SubmitError {
	fn from(other: stream::WriteError) -> Self {
		Self::BufferError(other)
	}
}

impl fmt::Display for SubmitError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::TimeError(e) => fmt::Display::fmt(e, f),
			Self::TimeMovingBackward => f.write_str("timestamp is going backward, have to drop"),
			Self::Duplicate => f.write_str("duplicate (rounded) timestamp"),
			Self::BufferError(e) => fmt::Display::fmt(e, f),
		}
	}
}

pub struct Descriptor {
	prev_t: DateTime<Utc>,
	component: SmartString,
	seq: u16,
	period: Duration,
	scratchpad: Arc<metric::RawData>,
	buffer: stream::InMemoryBuffer,
}

impl Descriptor {
	pub fn new(component: SmartString, period: Duration, slice: chrono::Duration) -> Self {
		Self {
			prev_t: chrono::MIN_DATETIME,
			seq: 0,
			period,
			component,
			scratchpad: Arc::new(metric::RawData::F64(
				metric::MaskedArray::masked_with_value(1, 0.),
			)),
			buffer: stream::InMemoryBuffer::new(slice),
		}
	}

	fn submit(
		&mut self,
		timestamp: DateTime<Utc>,
		path: metric::DevicePath,
		value: metric::Value,
	) -> Result<(), SubmitError> {
		let timestamp =
			timestamp.duration_trunc(chrono::Duration::from_std(self.period).unwrap())?;
		if timestamp < self.prev_t {
			return Err(SubmitError::TimeMovingBackward);
		}
		if timestamp == self.prev_t {
			return Err(SubmitError::Duplicate);
		}
		let seq_diff: u16 = match (timestamp - self.prev_t).num_nanoseconds() {
			Some(v) => {
				let diff = v / (self.period.as_nanos() as i64);
				assert!(diff >= 0);
				diff as u16
			}
			None => {
				// gigantic clock jump, that will also trip off the buffer so we can use whatever seq
				u16::MAX
			}
		};
		self.seq = self.seq.wrapping_add(seq_diff);
		self.prev_t = timestamp;
		{
			let array = Arc::make_mut(&mut self.scratchpad);
			match array {
				metric::RawData::F64(ref mut data) => {
					data.write_from(0, std::iter::once(value.magnitude));
				}
				// We create F64 at initialization time
				_ => unreachable!(),
			}
		}
		self.buffer.write(&metric::StreamBlock {
			t0: timestamp,
			seq0: self.seq,
			path: path,
			period: self.period,
			scale: metric::Value {
				magnitude: 1.,
				unit: value.unit,
			},
			data: self.scratchpad.clone(),
		})?;
		Ok(())
	}
}

struct StreamifyWorker();

impl StreamifyWorker {
	fn spawn(
		streams: HashMap<metric::DevicePath, Descriptor>,
		sample_source: mpsc::Receiver<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
	) {
		tokio::spawn(
			async move { StreamifyWorker::run(streams, sample_source, stream_sink).await },
		);
	}

	async fn run(
		mut streams: HashMap<metric::DevicePath, Descriptor>,
		mut source: mpsc::Receiver<payload::Sample>,
		sink: broadcast::Sender<payload::Stream>,
	) {
		loop {
			let mut readouts = match source.recv().await {
				Some(item) => item,
				None => return,
			};
			if readouts.len() == 0 {
				continue;
			}

			for readout in readouts.drain(..) {
				let descriptor = match streams.get_mut(&readout.path) {
					Some(v) => v,
					None => continue,
				};
				let value = match readout.components.get(&descriptor.component) {
					Some(v) => v,
					None => {
						warn!("component {:?} missing on sample for device {:?}, which is supposed to be converted to a stream", descriptor.component, readout.path);
						continue;
					}
				};
				match descriptor.submit(readout.timestamp, readout.path.clone(), value.clone()) {
					Ok(()) => (),
					Err(e) => warn!(
						"failed to streamify sample for device {:?}: {}",
						readout.path, e
					),
				};
				match descriptor.buffer.read_next() {
					Some(block) => match sink.send(Arc::new(block)) {
						Ok(_) => (),
						Err(_) => warn!("no receivers for streamified data"),
					},
					None => (),
				}
			}
		}
	}
}

pub struct Streamify {
	samples: Serializer<payload::Sample>,
	stream_zygote: broadcast::Sender<payload::Stream>,
}

impl Streamify {
	pub fn new(descriptors: HashMap<metric::DevicePath, Descriptor>) -> Self {
		let (samples, sample_source) = Serializer::new(128);
		let (stream_zygote, _) = broadcast::channel(128);
		StreamifyWorker::spawn(descriptors, sample_source, stream_zygote.clone());
		Self {
			samples,
			stream_zygote,
		}
	}
}

impl Source for Streamify {
	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.stream_zygote.subscribe()
	}

	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		null_receiver()
	}
}

impl Sink for Streamify {
	fn attach_source<'x>(&self, src: &'x dyn Source) {
		self.samples.attach(src.subscribe_to_samples());
	}
}
