use std::collections::HashMap;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, error, info, trace, warn};

use rand;
use rand::Rng;

use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use futures::sink::SinkExt;
use futures::stream::StreamExt;

use super::frame;

type FramedStream = tokio_util::codec::Framed<tokio::net::TcpStream, frame::FrameCodec>;

enum RecvEvent {
	SocketError,
	HardTimeout(Instant),
	DataFrame(frame::DataFrame),
}

#[derive(Debug)]
struct RecvSessionState {
	last_received: AtomicU64,
}

#[derive(Debug)]
pub struct SessionConfig {
	pub soft_timeout: Duration,
	pub hard_timeout: Duration,
	pub session_timeout: Duration,
}

impl RecvSessionState {
	async fn run(
		&self,
		cfg: Arc<SessionConfig>,
		mut socket: FramedStream,
		mut stop_ch: oneshot::Receiver<()>,
		event_ch: mpsc::Sender<RecvEvent>,
	) {
		let mut last_contact = Instant::now();
		loop {
			let now = Instant::now();
			let soft_deadline = last_contact + cfg.soft_timeout;
			let hard_deadline = last_contact + cfg.hard_timeout;
			if hard_deadline < now {
				// hard deadline elapsed; notify and exit
				// we don't care about the result really
				let _ = event_ch.send(RecvEvent::HardTimeout(last_contact)).await;
				return;
			}

			let soft_deadline_expired = soft_deadline < now;
			// TODO: send ping / ack request

			let timeout = if soft_deadline_expired {
				hard_deadline.duration_since(now)
			} else {
				soft_deadline.duration_since(now)
			};

			let frame = select! {
				_ = tokio::time::sleep(timeout) => {
					// timeout elapsed, iterate so that we can evaluate what went down
					continue;
				},
				_ = &mut stop_ch => {
					return;
				},
				result = socket.next() => {
					// if we are told to stop, that is because another connection has been received. we have to exit without processing any more data.
					match stop_ch.try_recv() {
						Ok(_) | Err(oneshot::error::TryRecvError::Closed) => return,
						Err(oneshot::error::TryRecvError::Empty) => (),
					}
					match result {
						None => {
							debug!("end of file on reception, exiting handler");
							let _ = event_ch.send(RecvEvent::SocketError).await;
							return;
						},
						Some(Err(e)) => {
							error!("read returned an error ({}), treating connection as dead", e);
							let _ = event_ch.send(RecvEvent::SocketError).await;
							return;
						},
						Some(Ok(frame)) => {
							last_contact = Instant::now();
							trace!("received frame: {:?}", frame);
							frame
						},
					}
				},
			};

			match frame {
				frame::Frame::ClientHello { .. }
				| frame::Frame::ServerHello { .. }
				| frame::Frame::Ack { .. } => {
					debug!(
						"closing connection because of protocol violation; received {:?}",
						frame
					);
					let _ = event_ch.send(RecvEvent::SocketError).await;
					return;
				}
				frame::Frame::Pong => (),
				frame::Frame::Ping => match socket.send(&frame::Frame::Pong).await {
					Ok(()) => (),
					Err(e) => {
						warn!("failed to send pong: {}", e);
						let _ = event_ch.send(RecvEvent::SocketError).await;
						return;
					}
				},
				frame::Frame::RequestAck => {
					match socket
						.send(&frame::Frame::Ack {
							last_received: self.last_received.load(Ordering::Relaxed),
						})
						.await
					{
						Ok(()) => (),
						Err(e) => {
							warn!("failed to send ack: {}", e);
							let _ = event_ch.send(RecvEvent::SocketError).await;
							return;
						}
					}
				}
				frame::Frame::Data(data) => {
					match event_ch.send(RecvEvent::DataFrame(data)).await {
						Ok(()) => (),
						Err(_) => {
							debug!("shutting down worker because the receiver is gone");
							return;
						}
					};
				}
			};
		}
	}
}

#[derive(Debug)]
struct RecvSession {
	state: Arc<RecvSessionState>,
	guard: oneshot::Sender<()>,
}

#[derive(Debug)]
struct ConnectionManager {
	sink: mpsc::Sender<(tokio::net::TcpStream, std::net::SocketAddr)>,
}

impl ConnectionManager {
	fn new(config: Arc<SessionConfig>) -> (Self, mpsc::Receiver<RecvEvent>) {
		let (sink, socket_src) = mpsc::channel(16);
		let (zygote, events) = mpsc::channel(8);
		let result = ConnectionManager { sink };
		tokio::spawn(async move {
			Self::run(config, socket_src, zygote).await;
		});
		(result, events)
	}

	async fn handshake(
		conn: tokio::net::TcpStream,
		connections: &mut HashMap<frame::ClientId, RecvSession>,
		config: Arc<SessionConfig>,
		zygote: &mpsc::Sender<RecvEvent>,
	) -> Result<(), StdIoError> {
		let mut ep = tokio_util::codec::Framed::new(conn, frame::FrameCodec());
		let client_id = match ep.next().await {
			None => {
				return Err(StdIoError::new(
					StdIoErrorKind::UnexpectedEof,
					format!("connection closed while reading ClientHello"),
				))
			}
			Some(v) => match v? {
				frame::Frame::ClientHello { client_id } => client_id,
				other => {
					return Err(StdIoError::new(
						StdIoErrorKind::InvalidData,
						format!("expected ClientHello, received {:?}", other),
					))
				}
			},
		};

		let (last_received, state) = match connections.get(&client_id) {
			Some(existing) => {
				let state = existing.state.clone();
				(Some(state.last_received.load(Ordering::Relaxed)), state)
			}
			None => {
				let new_state = Arc::new(RecvSessionState {
					// TODO: this is technically incorrect ...
					last_received: AtomicU64::new(0),
				});
				(None, new_state)
			}
		};

		ep.feed(&frame::Frame::ServerHello { last_received })
			.await?;
		ep.send(&frame::Frame::Ping).await?;

		match ep.next().await {
			None => {
				return Err(StdIoError::new(
					StdIoErrorKind::UnexpectedEof,
					format!("connection closed while reading Pong"),
				))
			}
			Some(v) => match v? {
				frame::Frame::Pong => (),
				other => {
					return Err(StdIoError::new(
						StdIoErrorKind::InvalidData,
						format!("expected Pong, received {:?}", other),
					))
				}
			},
		}

		let coro_state = state.clone();
		let (my_guard, their_guard) = oneshot::channel();
		let event_ch = zygote.clone();
		tokio::spawn(async move {
			coro_state.run(config, ep, their_guard, event_ch).await;
		});
		// if an old session existed, this insert will cause it to be dropped, thereby gracefully stopping the coroutine which was servicing it and cleaning up the socket and all that
		// TODO: maybe consider if the timing of this is right, but I think it is.
		connections.insert(
			client_id,
			RecvSession {
				state,
				guard: my_guard,
			},
		);
		Ok(())
	}

	async fn run(
		config: Arc<SessionConfig>,
		mut sockets: mpsc::Receiver<(tokio::net::TcpStream, std::net::SocketAddr)>,
		zygote: mpsc::Sender<RecvEvent>,
	) {
		let mut connections = HashMap::<frame::ClientId, RecvSession>::new();
		loop {
			let (stream, addr) = match sockets.recv().await {
				Some(v) => v,
				None => {
					debug!("shutting down connection manager coroutine");
					return;
				}
			};

			select! {
				_ = tokio::time::sleep(Duration::new(10, 0)) => {
					warn!("timeout during connection handshake");
					return;
				},
				conn = Self::handshake(stream, &mut connections, config.clone(), &zygote) => match conn {
					Ok(()) => {
						info!("successfully accepted and handshaked connection from {}", addr);
					},
					Err(e) => {
						warn!("dropping connection due to handshake error: {}", e);
						return;
					},
				},
			}
		}
	}

	fn try_send(&self, sock: tokio::net::TcpStream, addr: std::net::SocketAddr) -> bool {
		match self.sink.try_send((sock, addr)) {
			Err(mpsc::error::TrySendError::Full(_)) => false,
			Err(mpsc::error::TrySendError::Closed(_)) => {
				panic!("connection manager task crashed?!")
			}
			Ok(_) => true,
		}
	}
}

#[derive(Debug)]
struct RecvState {
	inner: tokio::net::TcpListener,
	connections: ConnectionManager,
	events: mpsc::Receiver<RecvEvent>,
	sink: broadcast::Sender<frame::DataFrame>,
	stop_ch: oneshot::Receiver<()>,
}

impl RecvState {
	pub fn new(
		inner: tokio::net::TcpListener,
		config: Arc<SessionConfig>,
		sink: broadcast::Sender<frame::DataFrame>,
		stop_ch: oneshot::Receiver<()>,
	) -> Self {
		let (connections, events) = ConnectionManager::new(config);
		Self {
			inner,
			connections,
			events,
			sink,
			stop_ch,
		}
	}

	async fn run(&mut self) {
		loop {
			select! {
				v = self.inner.accept() => match v {
					Ok((socket, addr)) => {
						if !self.connections.try_send(socket, addr) {
							warn!("connection manager overloaded, dropped connection from {}", addr)
						}
					},
					Err(e) => {
						error!("failed to accept connection (exiting!): {}", e);
						return;
					},
				},
				v = self.events.recv() => match v {
					None => {
						debug!("connection manager must've lost its zygote and all sockets are gone, shutting down");
						return;
					},
					Some(ev) => match ev {
						RecvEvent::SocketError => info!("lost a socket to a socket error :("),
						RecvEvent::HardTimeout(_) => info!("socket closed due to hard timeout"),
						RecvEvent::DataFrame(frame) => {
							let _ = self.sink.send(frame);
						},
					},
				},
				_ = &mut self.stop_ch => {
					debug!("RecvState peer dropped, exiting and taking all the things with me");
					return;
				},
			}
		}
	}
}

pub struct RecvSocket {
	zygote: broadcast::Sender<frame::DataFrame>,
	#[allow(dead_code)]
	guard: oneshot::Sender<()>,
}

impl RecvSocket {
	pub fn new(listener: tokio::net::TcpListener, cfg: Arc<SessionConfig>) -> Self {
		let (zygote, _) = broadcast::channel(8);
		let (guard, stop_ch) = oneshot::channel();
		let mut state = RecvState::new(listener, cfg, zygote.clone(), stop_ch);
		tokio::spawn(async move { state.run().await });
		Self { zygote, guard }
	}

	pub fn subscribe(&self) -> broadcast::Receiver<frame::DataFrame> {
		self.zygote.subscribe()
	}
}

struct SendState<T: tokio::net::ToSocketAddrs + Sync + Send + 'static> {
	client_id: frame::ClientId,
	data: mpsc::Receiver<frame::DataFrame>,
	addrs: T,
}

impl<T: tokio::net::ToSocketAddrs + Sync + Send + 'static> SendState<T> {
	pub fn new(data: mpsc::Receiver<frame::DataFrame>, addrs: T) -> Self {
		let client_id: u128 = rand::thread_rng().gen::<u128>();
		Self {
			client_id,
			data,
			addrs: addrs,
		}
	}

	async fn handshake(&mut self, ep: &mut FramedStream) -> Result<(), std::io::Error> {
		ep.send(&frame::Frame::ClientHello {
			client_id: self.client_id,
		})
		.await?;

		match ep.next().await {
			None => {
				return Err(StdIoError::new(
					StdIoErrorKind::UnexpectedEof,
					format!("connection closed while reading ServerHello"),
				))
			}
			Some(Ok(_)) => (),
			Some(Err(e)) => return Err(e),
		};

		match ep.next().await {
			None => {
				return Err(StdIoError::new(
					StdIoErrorKind::UnexpectedEof,
					format!("connection closed while reading initial Ping"),
				))
			}
			Some(Ok(_)) => (),
			Some(Err(e)) => return Err(e),
		};

		ep.send(&frame::Frame::Pong).await?;
		Ok(())
	}

	async fn socket_worker(&mut self, mut ep: FramedStream) -> Result<(), std::io::Error> {
		// TODO: add some kind of corking to request ACKs and stuff
		loop {
			select! {
				v = ep.next() => match v {
					None => return Err(StdIoError::new(
						StdIoErrorKind::UnexpectedEof,
						"connection closed",
					)),
					Some(Ok(frame_rx)) => match frame_rx {
						frame::Frame::ClientHello{..} | frame::Frame::ServerHello{..} | frame::Frame::RequestAck | frame::Frame::Data(_) => {
							return Err(StdIoError::new(
								StdIoErrorKind::InvalidData,
								"received invalid frame for sending endpoint",
							));
						},
						frame::Frame::Ping => {
							ep.send(&frame::Frame::Pong).await?;
						},
						frame::Frame::Pong => (),
						frame::Frame::Ack{..} => {
							// TODO: process ACK
							debug!("dropping ack response because it's not implemented :)");
						},
					},
					Some(Err(e)) => return Err(e),
				},
				v = self.data.recv() => match v {
					None => return Ok(()),
					Some(frame_tx) => {
						ep.send(&frame::Frame::Data(frame_tx)).await?;
					},
				},
			}
		}
	}

	pub async fn run(&mut self) {
		loop {
			let sock = match tokio::net::TcpStream::connect(&self.addrs).await {
				Ok(s) => s,
				Err(e) => {
					warn!(
						"failed to establish connection to receiver, retrying soon: {}",
						e
					);
					tokio::time::sleep(Duration::new(5, 0)).await;
					continue;
				}
			};
			let mut ep = tokio_util::codec::Framed::new(sock, frame::FrameCodec());
			match self.handshake(&mut ep).await {
				Ok(()) => (),
				Err(e) => {
					warn!("handshake failed ({}), retrying soon.", e);
					tokio::time::sleep(Duration::new(5, 0)).await;
					continue;
				}
			};
			match self.socket_worker(ep).await {
				Ok(()) => {
					info!("channel closed, exiting");
					return;
				}
				Err(e) => {
					debug!("lost client connection, reconnecting immediately: {}", e);
				}
			};
		}
	}
}

pub struct SendSocket {
	sink: mpsc::Sender<frame::DataFrame>,
}

impl SendSocket {
	pub fn new<T: tokio::net::ToSocketAddrs + Send + Sync + 'static>(addrs: T) -> Self {
		let (sink, data_ch) = mpsc::channel(8);
		let result = Self { sink };
		let mut state = SendState::new(data_ch, addrs);
		tokio::spawn(async move { state.run().await });
		result
	}

	pub async fn send(&self, frame: frame::DataFrame) {
		match self.sink.send(frame).await {
			Ok(()) => (),
			Err(_) => {
				panic!("processor task has crashed");
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::metric;
	use env_logger;

	use chrono::Utc;

	#[tokio::test]
	async fn test_sockets() {
		env_logger::init();

		let cfg = Arc::new(SessionConfig {
			soft_timeout: Duration::new(1, 0),
			hard_timeout: Duration::new(2, 0),
			session_timeout: Duration::new(3600, 0),
		});
		let recv_sock = tokio::net::TcpListener::bind(("127.0.0.1", 0u16))
			.await
			.unwrap();
		let local_addr = recv_sock.local_addr().unwrap();
		let recv_sock = RecvSocket::new(recv_sock, cfg.clone());

		let mut recv_ch = recv_sock.subscribe();

		let mut data = metric::Readout {
			timestamp: Utc::now(),
			path: metric::DevicePath {
				instance: "/some/device".into(),
				device_type: "magic".into(),
			},
			components: metric::OrderedVec::new(),
		};
		data.components.insert(
			"foo".into(),
			metric::Value {
				magnitude: 23.42,
				unit: metric::Unit::Celsius,
			},
		);

		let send_sock = SendSocket::new(local_addr);
		send_sock
			.send(frame::DataFrame::Readout(
				vec![Arc::new(data.clone())].into(),
			))
			.await;

		let received = recv_ch.recv().await;
		match received {
			Ok(frame::DataFrame::Readout(readout)) => {
				assert_eq!(*readout[0], data);
			}
			other => panic!("unexpected reception: {:?}", other),
		}
	}
}
