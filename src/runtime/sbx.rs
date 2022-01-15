use std::io;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, trace, warn};

#[cfg(feature = "serial")]
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::broadcast;
use tokio::sync::oneshot;

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};

use enum_map::{enum_map, EnumMap};

use bytes::Buf;
#[cfg(feature = "serial")]
use bytes::Bytes;

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

struct SBXSourceWorker {
	path_prefix: String,
	sample_sink: broadcast::Sender<payload::Sample>,
	#[allow(dead_code)]
	stream_sink: broadcast::Sender<payload::Stream>,
	stop_ch: oneshot::Receiver<()>,
	rtcifier: sbx::RangeRTC,
	stream_decoders: EnumMap<sbx::StreamKind, sbx::StreamDecoder<stream::InMemoryBuffer>>,
	buffer: Vec<Box<sbx::Message>>,
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

enum Error {
	ConnectionLost,
	Shutdown,
}

impl SBXSourceWorker {
	pub fn spawn_with_snurl(
		epf: EndpointFactory,
		path_prefix: String,
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
		stop_ch: oneshot::Receiver<()>,
	) -> io::Result<()> {
		let ep = Box::new(epf()?);

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

		let mut worker = Self {
			path_prefix,
			sample_sink,
			stream_sink,
			stop_ch,
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
		};
		tokio::spawn(async move {
			worker.run_with_snurl(ep, epf).await;
		});
		Ok(())
	}

	#[cfg(feature = "serial")]
	pub fn spawn_with_serialstream(
		src: tokio_serial::SerialStream,
		path_prefix: String,
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
		stop_ch: oneshot::Receiver<()>,
	) {
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

		let mut worker = Self {
			path_prefix,
			sample_sink,
			stream_sink,
			stop_ch,
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
		};
		let src = Box::new(src);
		tokio::spawn(async move {
			worker.run_with_serialstream(src).await;
		});
	}

	fn process_ready(&mut self, msg: sbx::Message) {
		let prefix = &self.path_prefix;
		let readouts = msg
			.readouts(&mut self.rtcifier)
			.map(|mut x| {
				x.path.instance.insert_str(0, prefix);
				Arc::new(x)
			})
			.collect();
		match self.sample_sink.send(readouts) {
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
					warn!(
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
						Some(block) => match self.stream_sink.send(Arc::new(block)) {
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

	fn process_sbx<R: Buf>(
		&mut self,
		timestamp: Option<DateTime<Utc>>,
		src: &mut R,
	) -> std::io::Result<()> {
		let msg = sbx::Message::read(src)?;
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
				self.process_ready(*msg);
			}
		}
		self.process_ready(msg);
		Ok(())
	}

	fn process_buf<R: Buf>(&mut self, src: &mut R) -> std::io::Result<()> {
		let hdr = sbx::EspMessageHeader::read(src)?;
		match hdr.type_ {
			sbx::EspMessageType::Status => {
				// TODO
			}
			sbx::EspMessageType::DataPassthrough => {
				let timestamp = if hdr.timestamp != 0 {
					Some(Utc.timestamp(hdr.timestamp as i64, 0))
				} else {
					None
				};
				self.process_sbx(timestamp, src)?;
			}
		}
		Ok(())
	}

	async fn process_one(&mut self, ep: &mut snurl::Endpoint) -> Result<(), Error> {
		tokio::select! {
			received = ep.recv_data() => {
				match received {
					// the socket was closed somehow? not sure how that could happen, but we need to shutdown then
					// well as it turns out it can happen when the network goes down, who would've thought.
					// which can happen when the router reboots because wifi.
					// which means we need to be smarter here than that.'
					None => {
						warn!("SBX SNURL endpoint closed unexpectedly");
						Err(Error::ConnectionLost)
					},
					Some(snurl::RecvItem::ResyncMarker) => {
						info!("resynchronized to SNURL sender, resetting all state");
						self.rtcifier.reset();
						for dec in self.stream_decoders.values_mut() {
							dec.reset();
						}
						if self.buffer.len() > 0 {
							warn!("dropping {} buffered frames because of resync", self.buffer.len());
							self.buffer.clear();
						}
						Ok(())
					},
					Some(snurl::RecvItem::Data(mut buf)) => {
						match self.process_buf(&mut buf) {
							Ok(()) => (),
							Err(e) => warn!("malformed packet received: {}", e),
						}
						Ok(())
					},
				}
			},
			_ = &mut self.stop_ch => {
				// SBXSource dropped, shut down
				// this will cascade down to our recipients then :)
				Err(Error::Shutdown)
			},
		}
	}

	async fn run_with_snurl(&mut self, mut ep: Box<snurl::Endpoint>, epf: EndpointFactory) {
		let epf = super::retry::Retry::new(epf, tokio::time::Duration::new(1, 0));
		loop {
			match self.process_one(&mut ep).await {
				Ok(()) => (),
				Err(Error::ConnectionLost) => {
					ep = Box::new(
						epf.obtain(|e| {
							warn!("failed to re-establish SNURL endpoint: {}", e);
						})
						.await,
					);
				}
				Err(Error::Shutdown) => return,
			}
		}
	}

	#[cfg(feature = "serial")]
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

	#[cfg(feature = "serial")]
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
		let mut frame = match self.decode_one_from_stream(src).await {
			Ok(v) => v,
			Err(e) => {
				warn!("failed to receive serial frame: {}. backing off.", e);
				tokio::time::sleep(tokio::time::Duration::new(0, 200)).await;
				return Ok(());
			}
		};
		let timestamp = Utc::now();
		match self.process_sbx(Some(timestamp), &mut frame) {
			Ok(()) => (),
			Err(e) => warn!("malformed packet received: {:?}", e),
		};
		Ok(())
	}

	#[cfg(feature = "serial")]
	async fn run_with_serialstream(&mut self, mut src: Box<tokio_serial::SerialStream>) {
		loop {
			match self.process_one_from_serial(&mut src).await {
				Ok(()) => (),
				Err(()) => return,
			}
		}
	}
}

impl SBXSource {
	pub fn new(epf: EndpointFactory, path_prefix: String) -> io::Result<Self> {
		let (sample_zygote, _) = broadcast::channel(384);
		let (stream_zygote, _) = broadcast::channel(1024);
		let (guard, stop_ch) = oneshot::channel();
		SBXSourceWorker::spawn_with_snurl(
			epf,
			path_prefix,
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
	pub fn with_serial(src: tokio_serial::SerialStream, path_prefix: String) -> Self {
		let (sample_zygote, _) = broadcast::channel(384);
		let (stream_zygote, _) = broadcast::channel(1024);
		let (guard, stop_ch) = oneshot::channel();
		SBXSourceWorker::spawn_with_serialstream(
			src,
			path_prefix,
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
