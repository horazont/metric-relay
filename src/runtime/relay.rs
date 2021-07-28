use std::sync::Arc;
use std::time::Duration;

use log::{error};

use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::mpsc;

use crate::relay;

use super::traits;
use super::payload;
use super::adapter::Serializer;

struct RelaySourceWorker {
	sample_sink: broadcast::Sender<payload::Sample>,
	#[allow(dead_code)]
	stream_sink: broadcast::Sender<payload::Stream>,
	stop_ch: oneshot::Receiver<()>,
	socket: relay::RecvSocket,
}

impl RelaySourceWorker {
	pub async fn run(&mut self) {
		let mut recv_ch = self.socket.subscribe();
		loop {
			select! {
				_ = &mut self.stop_ch => return,
				v = recv_ch.recv() => match v {
					Ok(relay::DataFrame::Readout(r)) => {
						// we cannot use the result as indicator because the parent struct only holds on to senders, not to receivers.
						// we use the stop_ch as a guard.
						let _ = self.sample_sink.send(r.into());
					},
					Ok(relay::DataFrame::Stream) => {
						unreachable!();
					},
					Err(_) => {
						// socket went down, close.
						error!("lost RecvSocket somehow");
						return;
					},
				},
			}
		}
	}
}

pub struct RelaySource {
	sample_zygote: broadcast::Sender<payload::Sample>,
	stream_zygote: broadcast::Sender<payload::Stream>,
	#[allow(dead_code)]
	guard: oneshot::Sender<()>,
}

impl RelaySource {
	pub fn new(socket: tokio::net::TcpListener) -> Self {
		let cfg = Arc::new(relay::SessionConfig{
			soft_timeout: Duration::new(5, 0),
			hard_timeout: Duration::new(30, 0),
			session_timeout: Duration::new(1800, 0),
		});
		let (guard, stop_ch) = oneshot::channel();
		let (sample_zygote, _) = broadcast::channel(8);
		let (stream_zygote, _) = broadcast::channel(8);
		let mut state = RelaySourceWorker{
			stream_sink: stream_zygote.clone(),
			sample_sink: sample_zygote.clone(),
			stop_ch,
			socket: relay::RecvSocket::new(socket, cfg),
		};
		tokio::spawn(async move {
			state.run().await
		});
		Self{
			stream_zygote,
			sample_zygote,
			guard,
		}
	}
}

impl traits::Source for RelaySource {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sample_zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		self.stream_zygote.subscribe()
	}
}

struct RelaySinkWorker {
	sock: relay::SendSocket,
	sample_source: mpsc::Receiver<payload::Sample>,
	stream_source: mpsc::Receiver<payload::Stream>,
}

impl RelaySinkWorker {
	async fn run(&mut self) {
		loop {
			select! {
				v = self.sample_source.recv() => match v {
					Some(readout) => {
						self.sock.send(relay::DataFrame::Readout(readout.into())).await
					},
					None => return,
				},
				v = self.stream_source.recv() => match v {
					Some(_) => unreachable!(),
					None => return,
				},
			}
		}
	}
}

pub struct RelaySink {
	samples: Serializer<payload::Sample>,
	stream: Serializer<payload::Stream>,
}

impl RelaySink {
	pub fn new<T: tokio::net::ToSocketAddrs + Sync + Send + 'static>(addrs: T) -> Self {
		let (samples, sample_source) = Serializer::new(8);
		let (stream, stream_source) = Serializer::new(8);
		let mut worker = RelaySinkWorker{
			sample_source,
			stream_source,
			sock: relay::SendSocket::new(addrs),
		};
		tokio::spawn(async move {
			worker.run().await
		});
		Self{
			samples,
			stream,
		}
	}
}

impl traits::Sink for RelaySink {
	fn attach_source<'x>(&mut self, src: &'x dyn traits::Source) {
		self.samples.attach(src.subscribe_to_samples());
		self.stream.attach(src.subscribe_to_streams());
	}
}
