use std::io;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, trace, warn};

#[cfg(feature = "serial")]
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::broadcast;
use tokio::sync::oneshot;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use enum_map::{enum_map, EnumMap};

#[cfg(feature = "serial")]
use bytes::Buf;
use bytes::Bytes;

use super::sbm::{HandlePassthrough, SbmSourceWorker, Sinks};

use crate::metric;
use crate::sbx;
use crate::sbx::{RTCifier, ReadoutIterable};
use crate::snurl;
use crate::stream;

use super::payload;
use super::traits;

pub struct SBXSource {
	sample_zygote: broadcast::Sender<payload::Sample>,
	stream_zygote: broadcast::Sender<payload::Stream>,
	#[allow(dead_code)]
	guard: oneshot::Sender<()>,
}

pub type EndpointFactory = Box<dyn Fn() -> io::Result<snurl::Endpoint> + Send + Sync + 'static>;

struct SbxHandler {
	path_prefix: String,
	rewrite_bme68x: bool,
	rtcifier: sbx::RangeRTC,
	stream_decoders: EnumMap<sbx::StreamKind, sbx::StreamDecoder<stream::InMemoryBuffer>>,
	buffer: Vec<Box<sbx::Message>>,
}

impl SbxHandler {
	fn new(path_prefix: String, rewrite_bme68x: bool) -> Self {
		let accel_period = Duration::from_millis(5);
		let accel_slice = ChronoDuration::seconds(60);
		let accel_scale = metric::Value {
			magnitude: 19.6133,
			unit: metric::Unit::MeterPerSqSecond,
		};

		let compass_period = Duration::from_millis(320);
		let compass_slice = ChronoDuration::seconds(64);
		let compass_scale = metric::Value {
			magnitude: 0.0002,
			unit: metric::Unit::Tesla,
		};

		Self {
			path_prefix,
			rewrite_bme68x,
			rtcifier: sbx::RangeRTC::default(),
			stream_decoders: enum_map! {
				sbx::StreamKind::AccelX => sbx::StreamDecoder::new(accel_period, stream::InMemoryBuffer::new(accel_slice), accel_scale.clone()),
				sbx::StreamKind::AccelY => sbx::StreamDecoder::new(accel_period, stream::InMemoryBuffer::new(accel_slice), accel_scale.clone()),
				sbx::StreamKind::AccelZ => sbx::StreamDecoder::new(accel_period, stream::InMemoryBuffer::new(accel_slice), accel_scale.clone()),
				sbx::StreamKind::CompassX => sbx::StreamDecoder::new(compass_period, stream::InMemoryBuffer::new(compass_slice), compass_scale.clone()),
				sbx::StreamKind::CompassY => sbx::StreamDecoder::new(compass_period, stream::InMemoryBuffer::new(compass_slice), compass_scale.clone()),
				sbx::StreamKind::CompassZ => sbx::StreamDecoder::new(compass_period, stream::InMemoryBuffer::new(compass_slice), compass_scale.clone()),
			},
			buffer: Vec::new(),
		}
	}

	fn process_ready(&mut self, msg: sbx::Message, sinks: &mut Sinks) {
		let prefix = &self.path_prefix;
		let rewrite_bme68x = self.rewrite_bme68x;
		let readouts = msg
			.readouts(&mut self.rtcifier)
			.map(|mut x| {
				x.path.instance.insert_str(0, prefix);
				if (x.path.device_type == "bme688" || x.path.device_type == "bme680")
					&& rewrite_bme68x
				{
					x.path.device_type = "bme280".into();
				}
				Arc::new(x)
			})
			.collect();
		match sinks.send_sample(readouts) {
			Ok(_) => (),
			Err(broadcast::error::SendError(readouts)) => {
				warn!(
					"dropped {} readouts because of no receivers",
					readouts.len()
				);
			}
		}

		match msg {
			sbx::Message::Status(msg) => {
				// we need to align the IMU streams here, and we can only do that after the main RTCifier synced, which is why we do it here.
				for (kind, dec) in self.stream_decoders.iter_mut() {
					let index = match kind {
						sbx::StreamKind::AccelX
						| sbx::StreamKind::AccelY
						| sbx::StreamKind::AccelZ => 0,
						sbx::StreamKind::CompassX
						| sbx::StreamKind::CompassY
						| sbx::StreamKind::CompassZ => 1,
					};
					let stream_info = &msg.imu_streams[index];
					if stream_info.period != 0 {
						let rtc = self.rtcifier.map_to_rtc(stream_info.timestamp);
						let seq = stream_info.sequence_number;
						let ready_pre = dec.ready();
						dec.align(rtc, seq);
						if !ready_pre && dec.ready() {
							info!("decoder for stream {:?} became ready", kind);
						}
					}
				}
			}
			sbx::Message::StreamData(ref streammsg) => {
				let decoder = &mut self.stream_decoders[streammsg.kind];
				if !decoder.ready() {
					debug!(
						"(re-)buffering stream message for {:?} because decoder is not ready",
						streammsg.kind
					);
					drop(streammsg);
					self.buffer.push(Box::new(msg));
				} else {
					match decoder.decode(streammsg.kind, &streammsg.data) {
						Ok(()) => trace!("samples sent to decoder successfully"),
						Err(e) => warn!("malformed stream message received: {}", e),
					};
					match decoder.read_next() {
						Some(block) => match sinks.send_stream(Arc::new(block)) {
							Ok(_) => (),
							Err(_) => warn!(
								"dropped stream data because no receivers were ready to receive"
							),
						},
						None => (),
					};
				}
			}
			_ => (),
		}
	}
}

impl HandlePassthrough for SbxHandler {
	fn handle(
		&mut self,
		timestamp: Option<DateTime<Utc>>,
		mut src: Bytes,
		sinks: &mut Sinks,
	) -> std::io::Result<()> {
		let msg = sbx::Message::read(&mut src)?;
		if let sbx::Message::Status(ref status) = msg {
			if let Some(rtc) = timestamp {
				self.rtcifier.align(rtc, status.uptime);
				if self.rtcifier.ready() {
					let mapped_rtc = self.rtcifier.map_to_rtc(status.uptime);
					let divergence = (rtc - mapped_rtc).num_seconds();
					trace!(
						"rtc mapping: uptime = {:>5}, remote rtc = {}, mapped rtc = {}, diff = {}",
						status.uptime,
						rtc,
						mapped_rtc,
						divergence
					);
					if divergence.abs() > 90 {
						warn!("rtcifier is off by {}, resetting (uptime = {}, remote rtc = {}, mapped rtc = {}): {:?}", divergence, status.uptime, rtc, mapped_rtc, self.rtcifier);
						self.rtcifier.reset();
						self.rtcifier.align(rtc, status.uptime);
					}
				} else {
					trace!(
						"rtc training: uptime = {:>5}, remote rtc = {}",
						status.uptime,
						rtc
					);
				}
			}
		}

		if !self.rtcifier.ready() {
			debug!("rtcifier is not ready yet, buffering message...");
			self.buffer.push(Box::new(msg));
			return Ok(());
		}

		if self.buffer.len() > 0 {
			let mut buffer = Vec::new();
			std::mem::swap(&mut buffer, &mut self.buffer);
			for msg in buffer.drain(..) {
				self.process_ready(*msg, sinks);
			}
		}
		self.process_ready(msg, sinks);
		Ok(())
	}

	fn reset(&mut self) {
		info!("resynchronized to SNURL sender, resetting all state");
		self.rtcifier.reset();
		for dec in self.stream_decoders.values_mut() {
			dec.reset();
		}
		if self.buffer.len() > 0 {
			warn!(
				"dropping {} buffered frames because of resync",
				self.buffer.len()
			);
			self.buffer.clear();
		}
	}
}

#[cfg(feature = "serial")]
async fn wait_for_api_frame<S: AsyncRead + Unpin>(src: &mut S) -> io::Result<()> {
	let mut buf = [0u8; 1];
	loop {
		src.read_exact(&mut buf[..]).await?;
		if buf[0] == 0x7e {
			return Ok(());
		}
	}
}

fn spawn_with_snurl(
	epf: EndpointFactory,
	path_prefix: String,
	rewrite_bme68x: bool,
	sample_sink: broadcast::Sender<payload::Sample>,
	stream_sink: broadcast::Sender<payload::Stream>,
	stop_ch: oneshot::Receiver<()>,
) -> io::Result<()> {
	let gw_path_prefix = path_prefix.clone() + "gateway/";
	let inner = SbxHandler::new(path_prefix, rewrite_bme68x);
	SbmSourceWorker::spawn_with_snurl(
		epf,
		gw_path_prefix,
		rewrite_bme68x,
		sample_sink,
		stream_sink,
		Some(Box::new(inner)),
		stop_ch,
	)
}

#[cfg(feature = "serial")]
struct SerialWorker {
	sinks: Sinks,
	inner: SbxHandler,
}

#[cfg(feature = "serial")]
impl SerialWorker {
	pub fn spawn(
		src: tokio_serial::SerialStream,
		path_prefix: String,
		rewrite_bme68x: bool,
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
		stop_ch: oneshot::Receiver<()>,
	) {
		let mut worker = Self {
			sinks: Sinks::wrap(sample_sink, stream_sink),
			inner: SbxHandler::new(path_prefix, rewrite_bme68x),
		};
		let src = Box::new(src);
		tokio::spawn(async move {
			worker.run_with_serialstream(src, stop_ch).await;
		});
	}

	async fn decode_one_from_stream<S: AsyncRead + Unpin>(
		&mut self,
		src: &mut S,
	) -> io::Result<Bytes> {
		let length = {
			let mut len_buf = [0u8; 2];
			src.read_exact(&mut len_buf[..]).await?;
			let mut len = &len_buf[..];
			len.get_u16()
		};
		if length >= 256 {
			warn!("oversized frame ({}), assuming desync", length);
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"oversized frame",
			));
		}
		if length < 13 {
			warn!("undersized frame ({}), assuming desync", length);
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"undersized frame",
			));
		}

		let mut buf = Vec::new();
		buf.resize(length as usize + 1, 0u8);
		src.read_exact(&mut buf[..]).await?;

		let mut checksum = 0u8;
		for b in buf.iter() {
			checksum = checksum.wrapping_add(*b);
		}
		if checksum != 0xff {
			warn!("incorrect checksum: 0x{:x} != 0xff", checksum);
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"checksum mismatch",
			));
		}

		let type_ = buf[0];
		if type_ != 0x10 {
			warn!("unexpected API frame type ({:?}), dropping", type_);
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"unsupported API frame type",
			));
		}

		// now extract the actual payload; it starts at offset 14
		let mut buf = Bytes::from(buf);
		buf.advance(14);
		// strip off the checksum
		buf.truncate(buf.len() - 1);
		debug!("received frame: {:?}", buf);
		Ok(buf)
	}

	async fn process_one_from_serial(
		&mut self,
		src: &mut tokio_serial::SerialStream,
	) -> Result<(), ()> {
		match wait_for_api_frame(src).await {
			Ok(v) => v,
			Err(e) => {
				warn!("failed to watch for API frame marker: {}. backing off.", e);
				tokio::time::sleep(tokio::time::Duration::new(0, 200)).await;
				return Ok(());
			}
		};
		let frame = match self.decode_one_from_stream(src).await {
			Ok(v) => v,
			Err(e) => {
				warn!("failed to receive serial frame: {}. backing off.", e);
				tokio::time::sleep(tokio::time::Duration::new(0, 200)).await;
				return Ok(());
			}
		};
		let timestamp = Utc::now();
		match self.inner.handle(Some(timestamp), frame, &mut self.sinks) {
			Ok(()) => (),
			Err(e) => warn!("malformed packet received: {:?}", e),
		};
		Ok(())
	}

	async fn run_with_serialstream(
		&mut self,
		mut src: Box<tokio_serial::SerialStream>,
		mut stop_ch: oneshot::Receiver<()>,
	) {
		loop {
			tokio::select! {
				result = self.process_one_from_serial(&mut src) => match result {
					Ok(()) => (),
					Err(()) => return,
				},
				_ = &mut stop_ch => return,
			}
		}
	}
}

impl SBXSource {
	pub fn new(
		epf: EndpointFactory,
		path_prefix: String,
		rewrite_bme68x: bool,
	) -> io::Result<Self> {
		let (sample_zygote, _) = broadcast::channel(384);
		let (stream_zygote, _) = broadcast::channel(1024);
		let (guard, stop_ch) = oneshot::channel();
		spawn_with_snurl(
			epf,
			path_prefix,
			rewrite_bme68x,
			sample_zygote.clone(),
			stream_zygote.clone(),
			stop_ch,
		)?;
		Ok(Self {
			sample_zygote,
			stream_zygote,
			guard,
		})
	}

	#[cfg(feature = "serial")]
	pub fn with_serial(
		src: tokio_serial::SerialStream,
		path_prefix: String,
		rewrite_bme68x: bool,
	) -> Self {
		let (sample_zygote, _) = broadcast::channel(384);
		let (stream_zygote, _) = broadcast::channel(1024);
		let (guard, stop_ch) = oneshot::channel();
		SerialWorker::spawn(
			src,
			path_prefix,
			rewrite_bme68x,
			sample_zygote.clone(),
			stream_zygote.clone(),
			stop_ch,
		);
		Self {
			sample_zygote,
			stream_zygote,
			guard,
		}
	}
}

impl traits::Source for SBXSource {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sample_zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.stream_zygote.subscribe()
	}
}
