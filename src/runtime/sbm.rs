use std::io;
use std::sync::Arc;

use log::{debug, trace, warn};

use tokio::sync::broadcast;
use tokio::sync::oneshot;

use chrono::{DateTime, TimeZone, Utc};

use bytes::Bytes;

use crate::sbm;
use crate::sbm::{DynSampleIterator, ReadoutIterable};
use crate::snurl;

use super::payload;
use super::traits;

pub type EndpointFactory = Box<dyn Fn() -> io::Result<snurl::Endpoint> + Send + Sync + 'static>;

pub struct Sinks {
	sample_sink: broadcast::Sender<payload::Sample>,
	stream_sink: broadcast::Sender<payload::Stream>,
}

impl Sinks {
	#[cfg(feature = "serial")]
	pub fn wrap(
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
	) -> Self {
		Self {
			sample_sink,
			stream_sink,
		}
	}

	#[inline(always)]
	pub fn send_sample(
		&mut self,
		sample: payload::Sample,
	) -> Result<usize, broadcast::error::SendError<payload::Sample>> {
		self.sample_sink.send(sample)
	}

	#[inline(always)]
	pub fn send_stream(
		&mut self,
		stream: payload::Stream,
	) -> Result<usize, broadcast::error::SendError<payload::Stream>> {
		self.stream_sink.send(stream)
	}
}

pub trait HandlePassthrough: Send + Sync {
	fn handle(
		&mut self,
		timestamp: Option<DateTime<Utc>>,
		data: Bytes,
		sinks: &mut Sinks,
	) -> io::Result<()>;

	fn reset(&mut self);
}

pub(super) struct SbmSourceWorker {
	path_prefix: String,
	rewrite_bme68x: bool,
	sinks: Sinks,
	passthrough: Option<Box<dyn HandlePassthrough + 'static>>,
	stop_ch: oneshot::Receiver<()>,
}

enum Error {
	ConnectionLost,
	Shutdown,
}

impl SbmSourceWorker {
	pub fn spawn_with_snurl(
		epf: EndpointFactory,
		path_prefix: String,
		rewrite_bme68x: bool,
		sample_sink: broadcast::Sender<payload::Sample>,
		stream_sink: broadcast::Sender<payload::Stream>,
		passthrough: Option<Box<dyn HandlePassthrough + 'static>>,
		stop_ch: oneshot::Receiver<()>,
	) -> io::Result<()> {
		let ep = Box::new(epf()?);

		let mut worker = Self {
			path_prefix,
			rewrite_bme68x,
			sinks: Sinks {
				sample_sink,
				stream_sink,
			},
			passthrough,
			stop_ch,
		};
		tokio::spawn(async move {
			worker.run_with_snurl(ep, epf).await;
		});
		Ok(())
	}

	fn process_buf(&mut self, mut src: Bytes) -> std::io::Result<()> {
		let hdr = sbm::EspMessageHeader::read(&mut src)?;
		trace!("processing message {:?} {:?}", hdr, src);
		let timestamp = if hdr.timestamp >= 86400 {
			Utc.timestamp_opt(hdr.timestamp as i64, 0).single()
		} else {
			None
		};
		let readout_iter: DynSampleIterator = match hdr.type_ {
			sbm::EspMessageType::Status => {
				if let Some(timestamp) = timestamp {
					let msg = sbm::EspStatus::read(&mut src)?;
					DynSampleIterator::wrap(msg.readouts(timestamp))
				} else {
					debug!(
						"dropping status message because timestamp {} is grossly wrong",
						hdr.timestamp
					);
					return Ok(());
				}
			}
			sbm::EspMessageType::DataPassthrough => {
				if let Some(pt) = self.passthrough.as_mut() {
					pt.handle(timestamp, src, &mut self.sinks)?;
				}
				return Ok(());
			}
			sbm::EspMessageType::Bme68x => {
				if let Some(timestamp) = timestamp {
					let msg = sbm::EspBme68xMessage::read(&mut src)?;
					DynSampleIterator::wrap(msg.readouts(timestamp))
				} else {
					debug!(
						"dropping sensor message because timestamp {} is grossly wrong",
						hdr.timestamp
					);
					return Ok(());
				}
			}
		};
		let prefix = &self.path_prefix;
		let rewrite_bme68x = self.rewrite_bme68x;
		let readouts = readout_iter
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
		let _ = self.sinks.send_sample(readouts);
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
						if let Some(pt) = self.passthrough.as_mut() {
							pt.reset();
						}
						Ok(())
					},
					Some(snurl::RecvItem::Data(buf)) => {
						match self.process_buf(buf) {
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
}

pub struct MininodeSource {
	sample_zygote: broadcast::Sender<payload::Sample>,
	stream_zygote: broadcast::Sender<payload::Stream>,
	#[allow(dead_code)]
	guard: oneshot::Sender<()>,
}

impl MininodeSource {
	pub fn new(
		epf: EndpointFactory,
		path_prefix: String,
		rewrite_bme68x: bool,
	) -> io::Result<Self> {
		let (sample_zygote, _) = broadcast::channel(384);
		let (stream_zygote, _) = broadcast::channel(1024);
		let (guard, stop_ch) = oneshot::channel();
		SbmSourceWorker::spawn_with_snurl(
			epf,
			path_prefix,
			rewrite_bme68x,
			sample_zygote.clone(),
			stream_zygote.clone(),
			None,
			stop_ch,
		)?;
		Ok(Self {
			sample_zygote,
			stream_zygote,
			guard,
		})
	}
}

impl traits::Source for MininodeSource {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sample_zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.stream_zygote.subscribe()
	}
}
