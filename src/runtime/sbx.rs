use std::sync::Arc;

use log::{warn, info};

use tokio::sync::broadcast;
use tokio::sync::oneshot;

use chrono::{Utc, TimeZone};

use bytes::{Buf};

use crate::snurl;
use crate::sbx;
use crate::sbx::ReadoutIterable;

use super::traits;
use super::payload;

pub struct SBXSource {
	sample_sender: broadcast::Sender<payload::Sample>,
	stream_sender: broadcast::Sender<payload::Stream>,
	#[allow(dead_code)]
	close_guard: oneshot::Sender<()>,
}

struct SBXSourceWorker {
	path_prefix: String,
	ep: snurl::Endpoint,
	sample_sender: broadcast::Sender<payload::Sample>,
	#[allow(dead_code)]
	stream_sender: broadcast::Sender<payload::Stream>,
	close_guard: oneshot::Receiver<()>,
	rtcifier: sbx::RTCifier,
	aligned: bool,
}

impl SBXSourceWorker {
	fn process_sbx<R: Buf>(&mut self, esp_timestamp: u32, src: &mut R) -> std::io::Result<()> {
		let msg = sbx::Message::read(src)?;
		if let sbx::Message::Status(ref status) = msg {
			if esp_timestamp != 0 {
				let rtc = Utc.timestamp(esp_timestamp as i64, 0);
				self.rtcifier.align(rtc, status.uptime);
				let mapped_rtc = self.rtcifier.map_to_rtc(status.uptime);
				let divergence = (rtc - mapped_rtc).num_seconds();
				if divergence.abs() > 90 {
					warn!("rtcifier is off by {}, resetting", divergence);
					self.rtcifier.reset();
					self.rtcifier.align(rtc, status.uptime);
					// TODO: what to do with the buffer? what if we have resets while learning the counter values? we cannot really rely on that in any way... and actually have to drop packets until we have an RTC sync.
					// OR we need to be informed by SNURL about resyncs, that'd be ok too, then we can reset our RTC when it happens and flush the buffer
					self.aligned = false;
				} else {
					if !self.aligned {
						info!("RTC re-synchronized (uptime = {}, rtc = {}, mapped_rtc = {})", status.uptime, rtc, mapped_rtc);
					}
					self.aligned = true;
				}
			}
		}

		if !self.aligned {
			// TODO: buffer, but see the comment above.
			return Ok(())
		}

		let mut dropped = 0usize;
		for mut readout in msg.readouts(&mut self.rtcifier) {
			readout.path.instance.insert_str(0, &self.path_prefix[..]);
			match self.sample_sender.send(Arc::new(readout)) {
				Ok(_) => (),
				Err(_) => dropped += 1,
			}
		}
		if dropped > 0 {
			warn!("dropped {} readouts because of no receivers", dropped);
		}
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
					Some(mut buf) => {
						match self.process_buf(&mut buf) {
							Ok(()) => (),
							Err(e) => warn!("malformed packet received: {}", e),
						}
						Ok(())
					},
				}
			},
			_ = &mut self.close_guard => {
				// SBXSource dropped, shut down
				// this will cascade down to our recipients then :)
				Err(())
			},
		}
	}
}

impl SBXSource {
	pub fn new(ep: snurl::Endpoint, path_prefix: String) -> Self {
		let (sample_sender, _) = broadcast::channel(384);
		let (stream_sender, _) = broadcast::channel(1024);
		let (closed_sender, closed_receiver) = oneshot::channel();
		let result = Self{
			stream_sender,
			sample_sender,
			close_guard: closed_sender,
		};
		result.spawn_into_background(
			ep,
			path_prefix,
			closed_receiver,
		);
		result
	}

	fn spawn_into_background(
			&self,
			ep: snurl::Endpoint,
			path_prefix: String,
			close_guard: oneshot::Receiver<()>) {
		let stream_sender = self.stream_sender.clone();
		let sample_sender = self.sample_sender.clone();
		tokio::spawn(async move {
			let mut data = SBXSourceWorker{
				path_prefix,
				ep,
				sample_sender,
				stream_sender,
				close_guard,
				rtcifier: sbx::RTCifier::default(),
				aligned: false,
			};
			loop {
				match data.process_one().await {
					Ok(()) => (),
					Err(()) => return,
				}
			}
		});
	}
}

impl traits::Source for SBXSource {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sample_sender.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.stream_sender.subscribe()
	}
}
