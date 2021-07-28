use std::ops::Deref;
use std::sync::Arc;

use log::trace;

use bytes::{Buf, BytesMut, BufMut};

use tokio_util::codec::{Decoder, Encoder};

use bincode;
use bincode::Options;

use serde;
use serde::{Deserializer, Serializer};
use serde_derive::{Deserialize, Serialize};

use crate::metric;

#[derive(Debug, Clone)]
pub struct ReadoutWrap(Arc<metric::Readout>);

impl<'de> serde::Deserialize<'de> for ReadoutWrap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let r = metric::Readout::deserialize(deserializer)?;
        Ok(ReadoutWrap(Arc::new(r)))
    }
}

impl serde::Serialize for ReadoutWrap {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
		where S: Serializer
	{
		(*self.0).serialize(serializer)
	}
}

impl Deref for ReadoutWrap {
	type Target = Arc<metric::Readout>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<ReadoutWrap> for Arc<metric::Readout> {
	fn from(other: ReadoutWrap) -> Self {
		other.0
	}
}

impl From<Arc<metric::Readout>> for ReadoutWrap {
	fn from(other: Arc<metric::Readout>) -> Self {
		Self(other)
	}
}

impl From<metric::Readout> for ReadoutWrap {
	fn from(other: metric::Readout) -> Self {
		Self(Arc::new(other))
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DataFrame {
	Readout(ReadoutWrap),
	Stream,
}

pub type ClientId = u128;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Frame {
	ClientHello{
		// chosen at startup by the client, once, randomly.
		client_id: ClientId,
	},
	ServerHello{
		// sequence number of the last received data frame, if and only if the
		// server has seen the client before and its session has not expired.
		last_received: Option<u64>,
	},
	Ping,
	Pong,
	Data(DataFrame),
	RequestAck,
	Ack{
		last_received: u64,
	},
}

pub struct FrameCodec();

const MAX_FRAME_SIZE: usize = 65535;

impl Decoder for FrameCodec {
	type Item = Frame;
	type Error = std::io::Error;

	fn decode(
		&mut self,
		src: &mut BytesMut
	) -> Result<Option<Self::Item>, Self::Error> {
		if src.len() < 4 {
			return Ok(None)
		}

		let mut length_bytes = [0u8; 4];
		length_bytes.copy_from_slice(&src[..4]);
		let length = u32::from_le_bytes(length_bytes) as usize;
		if length > MAX_FRAME_SIZE {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!("frame size {} exceeds maximum frame size {}", length, MAX_FRAME_SIZE),
			));
		}

		if src.len() < 4 + length {
			// need more data
			src.reserve(4 + length - src.len());
			return Ok(None)
		}

		let frame = match bincode::DefaultOptions::new()
			.with_little_endian()
			.with_limit(MAX_FRAME_SIZE as u64)
			.deserialize(&src[4..4+length]) {
			Ok(f) => f,
			Err(e) => return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				e,
			)),
		};
		src.advance(4 + length);
		trace!("decoded frame: {:?}", frame);
		Ok(Some(frame))
	}
}

impl Encoder<&Frame> for FrameCodec {
	type Error = std::io::Error;

	fn encode(&mut self, item: &Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
		let config = bincode::DefaultOptions::new()
			.with_little_endian()
			.with_limit(MAX_FRAME_SIZE as u64);

		let len = match config.serialized_size(&item) {
			Err(e) => return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				e,
			)),
			Ok(l) => l,
		};
		if len > MAX_FRAME_SIZE as u64 {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				format!("data size {} would exceed maximum frame size {}", len, MAX_FRAME_SIZE),
			))
		}

		dst.reserve(len as usize + 4);
		dst.put_u32_le(len as u32);

		trace!("encoding frame in {} bytes: {:?}", len, item);

		match config.serialize_into(dst.writer(), &item) {
			Err(e) => match *e {
				bincode::ErrorKind::Io(ioe) => Err(ioe),
				other => Err(std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					other,
				)),
			},
			Ok(()) => Ok(()),
		}
	}
}

#[cfg(test)]
mod test_codec {
	use super::*;
	use tokio::net::UnixStream;
	use tokio_util::codec::Framed;
	use futures::sink::SinkExt;
	use futures::stream::StreamExt;

	#[tokio::test]
	async fn test_codec() {
		let (s1, s2) = UnixStream::pair().unwrap();
		let mut ep1 = Framed::new(s1, FrameCodec());
		let mut ep2 = Framed::new(s2, FrameCodec());

		{
			let test_client_id = 0xdeadbeeff00ba42342;
			let test_send = Frame::ClientHello{
				client_id: test_client_id,
			};
			ep1.send(&test_send).await.unwrap();
			let test_recv = ep2.next().await.unwrap().unwrap();
			match test_recv {
				Frame::ClientHello{client_id} => {
					assert_eq!(client_id, test_client_id);
				},
				other => panic!("unexpected frame: {:?}", other),
			}
		}

		{
			let test_last_received = 0x2342;
			let test_send = Frame::ServerHello{
				last_received: Some(test_last_received),
			};
			ep2.send(&test_send).await.unwrap();
			let test_recv = ep1.next().await.unwrap().unwrap();
			match test_recv {
				Frame::ServerHello{last_received} => {
					assert_eq!(last_received, Some(test_last_received));
				},
				other => panic!("unexpected frame: {:?}", other),
			}
		}
	}
}
