#[cfg(feature = "debug")]
use std::fmt;
#[cfg(feature = "debug")]
use std::sync::Arc;

#[allow(unused_imports)]
use log::{warn, debug, trace};

use tokio::sync::mpsc;
use tokio::sync::broadcast;

#[cfg(feature = "debug")]
use crate::stream;

#[cfg(feature = "debug")]
use super::payload;


pub struct Serializer<T: 'static + Clone + Send> {
	sink: mpsc::Sender<T>,
}

impl<T: 'static + Clone + Send> Serializer<T> {
	pub fn new(depth: usize) -> (Self, mpsc::Receiver<T>) {
		let (sender, receiver) = mpsc::channel(depth);
		(Self{sink: sender}, receiver)
	}

	pub fn attach(&self, mut src: broadcast::Receiver<T>) {
		let sink = self.sink.clone();
		tokio::spawn(async move {
			loop {
				let item = match src.recv().await {
					// sending side closed, disconnect
					Err(broadcast::error::RecvError::Closed) => {
						debug!("serializer stream exiting because source got closed");
						return
					},
					Err(broadcast::error::RecvError::Lagged(nlost)) => {
						warn!("serializer was too slow; lost {} items", nlost);
						continue
					},
					Ok(item) => item,
				};
				match sink.send(item).await {
					Ok(()) => (),
					// receiving side closed, disconnect
					Err(_) => {
						debug!("serializer stream exiting because destination got closed");
						return
					},
				}
			}
		});
	}
}


#[cfg(feature = "debug")]
#[derive(Debug)]
pub enum BufferedStreamError {
	BufferWriteError(stream::WriteError),
	SendError(broadcast::error::SendError<payload::Stream>),
}

#[cfg(feature = "debug")]
impl From<broadcast::error::SendError<payload::Stream>> for BufferedStreamError {
	fn from(other: broadcast::error::SendError<payload::Stream>) -> Self {
		Self::SendError(other)
	}
}

#[cfg(feature = "debug")]
impl From<stream::WriteError> for BufferedStreamError {
	fn from(other: stream::WriteError) -> Self {
		Self::BufferWriteError(other)
	}
}

#[cfg(feature = "debug")]
impl fmt::Display for BufferedStreamError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::BufferWriteError(ref v) => v.fmt(f),
			Self::SendError(ref v) => v.fmt(f),
		}
	}
}

#[cfg(feature = "debug")]
impl std::error::Error for BufferedStreamError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::BufferWriteError(ref v) => Some(v),
			Self::SendError(ref v) => Some(v),
		}
	}
}

#[cfg(feature = "debug")]
pub struct BufferedStream<T: stream::StreamBuffer + ?Sized> {
	buffer: Box<T>,
	sink: broadcast::Sender<payload::Stream>,
}

#[cfg(feature = "debug")]
impl<T: stream::StreamBuffer + ?Sized> BufferedStream<T> {
	pub fn new(buffer: Box<T>, sink: broadcast::Sender<payload::Stream>) -> Self {
		Self{buffer, sink}
	}

	pub fn send(&mut self, block: payload::Stream) -> Result<(), BufferedStreamError> {
		trace!("writing block to buffer: path={}  t0={}  seq0={}  len={}", block.path, block.t0, block.seq0, block.data.len());
		self.buffer.write(&block)?;
		while let Some(block) = self.buffer.read_next() {
			trace!("emitting block from buffer: path={}  t0={}  seq0={}  len={}", block.path, block.t0, block.seq0, block.data.len());
			self.sink.send(Arc::new(block))?;
		}
		Ok(())
	}
}
