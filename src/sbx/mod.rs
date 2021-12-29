use bytes::Buf;
use std::convert::TryInto;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind, Result as StdIoResult};

mod frame;
mod generators;
mod rtcifier;
mod stream;

pub use frame::{
	EspMessageHeader, EspMessageType, EspStatus, SbxBME280Message, SbxDS18B20Message,
	SbxLightMessage, SbxMessageType, SbxNoiseMessage, SbxStatusMessage, SbxStreamMessage,
};

pub use generators::ReadoutIterable;
#[cfg(feature = "unstable-rtcs")]
pub use rtcifier::{FilteredRTC, RangeRTCLiori, RangeRTCv2};
pub use rtcifier::{LinearRTC, RTCifier, RangeRTC};

pub use stream::{StreamDecoder, StreamKind};

#[derive(Debug, Clone)]
pub enum ReadoutMessage {
	DS18B20(SbxDS18B20Message),
	Light(SbxLightMessage),
	BME280(SbxBME280Message),
	Noise(SbxNoiseMessage),
}

impl From<SbxDS18B20Message> for ReadoutMessage {
	fn from(other: SbxDS18B20Message) -> Self {
		Self::DS18B20(other)
	}
}

impl From<SbxLightMessage> for ReadoutMessage {
	fn from(other: SbxLightMessage) -> Self {
		Self::Light(other)
	}
}

impl From<SbxBME280Message> for ReadoutMessage {
	fn from(other: SbxBME280Message) -> Self {
		Self::BME280(other)
	}
}

impl From<SbxNoiseMessage> for ReadoutMessage {
	fn from(other: SbxNoiseMessage) -> Self {
		Self::Noise(other)
	}
}

impl<'x, T: rtcifier::RTCifier + 'static> generators::ReadoutIterable<'x, T> for ReadoutMessage {
	type GenIter = generators::DynSampleIterator<'x>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		match self {
			Self::DS18B20(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::BME280(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::Noise(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::Light(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
		}
	}
}

#[derive(Debug, Clone)]
pub struct StreamMessage {
	pub kind: StreamKind,
	pub data: SbxStreamMessage,
}

impl StreamMessage {
	pub fn build(msgtype: SbxMessageType, data: SbxStreamMessage) -> StreamMessage {
		let kind: stream::StreamKind = msgtype.try_into().expect("stream message kind");
		StreamMessage { kind, data }
	}
}

impl<'x, T: rtcifier::RTCifier> generators::ReadoutIterable<'x, T> for StreamMessage {
	type GenIter = generators::DynSampleIterator<'x>;

	fn readouts(&'x self, _rtcifier: &'x mut T) -> Self::GenIter {
		Self::GenIter::wrap(generators::Empty())
	}
}

#[derive(Debug, Clone)]
pub enum Message {
	Status(SbxStatusMessage),
	ReadoutData(ReadoutMessage),
	StreamData(StreamMessage),
}

impl From<SbxStatusMessage> for Message {
	fn from(other: SbxStatusMessage) -> Self {
		Self::Status(other)
	}
}

impl<T: Into<ReadoutMessage>> From<T> for Message {
	fn from(other: T) -> Self {
		Self::ReadoutData(other.into())
	}
}

impl From<StreamMessage> for Message {
	fn from(other: StreamMessage) -> Self {
		Self::StreamData(other)
	}
}

impl Message {
	pub fn read<B: Buf>(r: &mut B) -> StdIoResult<Message> {
		use SbxMessageType::*;
		let type_ = SbxMessageType::read(r)?;
		match type_ {
			Status => Ok(SbxStatusMessage::read(r)?.into()),
			SensorDS18B20 => Ok(SbxDS18B20Message::read(r)?.into()),
			SensorNoise => Ok(SbxNoiseMessage::read(r)?.into()),
			SensorBME280 => Ok(SbxBME280Message::read(r)?.into()),
			SensorLight => Ok(SbxLightMessage::read(r)?.into()),
			SensorStreamAccelX | SensorStreamAccelY | SensorStreamAccelZ | SensorStreamCompassX
			| SensorStreamCompassY | SensorStreamCompassZ => {
				Ok(StreamMessage::build(type_, SbxStreamMessage::read(r)?).into())
			}
			_ => Err(StdIoError::new(
				StdIoErrorKind::InvalidData,
				"unsupported message type",
			)),
		}
	}
}

impl<'x, T: rtcifier::RTCifier + 'static> generators::ReadoutIterable<'x, T> for Message {
	type GenIter = generators::DynSampleIterator<'x>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		match self {
			Self::Status(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::ReadoutData(msg) => msg.readouts(rtcifier),
			Self::StreamData(msg) => msg.readouts(rtcifier),
		}
	}
}
