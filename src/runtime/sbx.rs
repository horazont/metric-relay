use std::sync::Arc;
use std::time::Duration;

use log::{warn, info, debug, trace};

use tokio::sync::broadcast;
use tokio::sync::oneshot;

use chrono::{Utc, TimeZone, Duration as ChronoDuration};

use enum_map::{enum_map, EnumMap};

use bytes::{Buf};

use crate::metric;
use crate::snurl;
use crate::sbx;
use crate::stream;
use crate::sbx::{ReadoutIterable, RTCifier};

use super::traits;
use super::payload;

pub struct SBXSource {
	sample_zygote: broadcast::Sender<payload::Sample>,
	stream_zygote: broadcast::Sender<payload::Stream>,
	#[allow(dead_code)]
	guard: oneshot::Sender<()>,
}

struct SBXSourceWorker {
	path_prefix: String,
	ep: snurl::Endpoint,
	sample_sink: broadcast::Sender<payload::Sample>,
	#[allow(dead_code)]
	stream_sink: broadcast::Sender<payload::Stream>,
	stop_ch: oneshot::Receiver<()>,
	rtcifier: sbx::RangeRTC,
	stream_decoders: EnumMap<sbx::StreamKind, sbx::StreamDecoder<stream::InMemoryBuffer>>,
	buffer: Vec<Box<sbx::Message>>,
}

impl SBXSourceWorker {
	pub fn spawn(
		ep: snurl::Endpoint,
		path_prefix: String,
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
		stop_ch: oneshot::Receiver<()>)
	{
		let accel_period = Duration::from_millis(5);
		let accel_slice = ChronoDuration::seconds(60);
		let accel_scale = metric::Value{
			magnitude: 19.6133,
			unit: metric::Unit::MeterPerSqSecond,
		};

		let compass_period = Duration::from_millis(320);
		let compass_slice = ChronoDuration::seconds(64);
		let compass_scale = metric::Value{
			magnitude: 0.0002,
			unit: metric::Unit::Tesla,
		};

		let mut worker = Self{
			path_prefix,
			ep,
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
			worker.run().await;
		});
	}

	fn process_ready(&mut self, msg: sbx::Message) {
		let prefix = &self.path_prefix;
		let readouts = msg.readouts(&mut self.rtcifier).map(|mut x| {
			x.path.instance.insert_str(0, prefix);
			Arc::new(x)
		}).collect();
		match self.sample_sink.send(readouts) {
			Ok(_) => (),
			Err(broadcast::error::SendError(readouts)) => {
				warn!("dropped {} readouts because of no receivers", readouts.len());
			},
		}

		match msg {
			sbx::Message::Status(msg) => {
				// we need to align the IMU streams here, and we can only do that after the main RTCifier synced, which is why we do it here.
				for (kind, dec) in self.stream_decoders.iter_mut() {
					let index = match kind {
						sbx::StreamKind::AccelX | sbx::StreamKind::AccelY | sbx::StreamKind::AccelZ => 0,
						sbx::StreamKind::CompassX | sbx::StreamKind::CompassY | sbx::StreamKind::CompassZ => 1,
					};
					let stream_info = &msg.imu_streams[index];
					let rtc = self.rtcifier.map_to_rtc(stream_info.timestamp);
					let seq = stream_info.sequence_number;
					let ready_pre = dec.ready();
					dec.align(rtc, seq);
					if !ready_pre && dec.ready() {
						info!("decoder for stream {:?} became ready", kind);
					}
				}
			},
			sbx::Message::StreamData(ref streammsg) => {
				let decoder = &mut self.stream_decoders[streammsg.kind];
				if !decoder.ready() {
					warn!("(re-)buffering stream message for {:?} because decoder is not ready", streammsg.kind);
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
							Err(_) => warn!("dropped stream data because no receivers were ready to receive"),
						},
						None => (),
					};
				}
			},
			_ => (),
		}
	}

	fn process_sbx<R: Buf>(&mut self, esp_timestamp: u32, src: &mut R) -> std::io::Result<()> {
		let msg = sbx::Message::read(src)?;
		if let sbx::Message::Status(ref status) = msg {
			if esp_timestamp != 0 {
				let rtc = Utc.timestamp(esp_timestamp as i64, 0);
				self.rtcifier.align(rtc, status.uptime);
				if self.rtcifier.ready() {
					let mapped_rtc = self.rtcifier.map_to_rtc(status.uptime);
					let divergence = (rtc - mapped_rtc).num_seconds();
					if divergence.abs() > 90 {
						warn!("rtcifier is off by {}, resetting", divergence);
						self.rtcifier.reset();
						self.rtcifier.align(rtc, status.uptime);
					}
					trace!("rtc divergence: {}", divergence);
				}
			}
		}

		if !self.rtcifier.ready() {
			debug!("rtcifier is not ready yet, buffering message...");
			self.buffer.push(Box::new(msg));
			return Ok(())
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
			},
			sbx::EspMessageType::DataPassthrough => {
				self.process_sbx(hdr.timestamp, src)?;
			}
		}
		Ok(())
	}

	async fn process_one(&mut self) -> Result<(), ()> {
		tokio::select! {
			received = self.ep.recv_data() => {
				match received {
					// the socket was closed somehow? not sure how that could happen, but we need to shutdown then
					None => {
						warn!("SBX SNURL endpoint closed unexpectedly");
						Err(())
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
				Err(())
			},
		}
	}

	async fn run(&mut self) {
		loop {
			match self.process_one().await {
				Ok(()) => (),
				Err(()) => return,
			}
		}
	}
}

impl SBXSource {
	pub fn new(ep: snurl::Endpoint, path_prefix: String) -> Self {
		let (sample_zygote, _) = broadcast::channel(384);
		let (stream_zygote, _) = broadcast::channel(1024);
		let (guard, stop_ch) = oneshot::channel();
		SBXSourceWorker::spawn(
			ep,
			path_prefix,
			sample_zygote.clone(),
			stream_zygote.clone(),
			stop_ch,
		);
		Self{
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
