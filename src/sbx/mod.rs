use std::io::{Result as StdIoResult, Error as StdIoError, ErrorKind as StdIoErrorKind};
use bytes::Buf;

mod frame;
mod rtcifier;
mod generators;
mod bme280;

pub use frame::{
	EspMessageHeader,
	EspMessageType,
	EspStatus,
	SbxMessageType,
	SbxDS18B20Message,
	SbxLightMessage,
	SbxStatusMessage,
	SbxBME280Message,
	SbxNoiseMessage,
	SbxStreamMessage,
};

pub use rtcifier::RTCifier;
pub use generators::ReadoutIterable;

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

impl<'x> generators::ReadoutIterable<'x> for ReadoutMessage {
	type GenIter = generators::DynSampleIterator<'x>;

	fn readouts(&'x self, rtcifier: &'x mut rtcifier::RTCifier) -> Self::GenIter {
		match self {
			Self::DS18B20(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::BME280(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::Noise(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::Light(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub enum IMUSensor {
	Accel,
	Compass,
}

#[derive(Debug, Clone, Copy)]
pub enum IMUAxis {
	X, Y, Z,
}

#[derive(Debug, Clone)]
pub struct StreamMessage {
	pub sensor: IMUSensor,
	pub axis: IMUAxis,
	pub data: SbxStreamMessage,
}

impl StreamMessage {
	pub fn build(msgtype: SbxMessageType, data: SbxStreamMessage) -> StreamMessage {
		use IMUSensor::*;
		use IMUAxis::*;
		use SbxMessageType::*;
		let (sensor, axis) = match msgtype {
			SensorStreamAccelX => (Accel, X),
			SensorStreamAccelY => (Accel, Y),
			SensorStreamAccelZ => (Accel, Z),
			SensorStreamCompassX => (Compass, X),
			SensorStreamCompassY => (Compass, Y),
			SensorStreamCompassZ => (Compass, Z),
			other => panic!("invalid stream message type: {:?}", other),
		};
		StreamMessage{
			sensor,
			axis,
			data,
		}
	}
}

impl<'x> generators::ReadoutIterable<'x> for StreamMessage {
	type GenIter = generators::DynSampleIterator<'x>;

	fn readouts(&'x self, _rtcifier: &'x mut rtcifier::RTCifier) -> Self::GenIter {
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
			SensorStreamAccelX | SensorStreamAccelY | SensorStreamAccelZ | SensorStreamCompassX | SensorStreamCompassY | SensorStreamCompassZ => {
				Ok(StreamMessage::build(type_, SbxStreamMessage::read(r)?).into())
			}
			_ => Err(StdIoError::new(StdIoErrorKind::InvalidData, "unsupported message type")),
		}
	}
}

impl<'x> generators::ReadoutIterable<'x> for Message {
	type GenIter = generators::DynSampleIterator<'x>;

	fn readouts(&'x self, rtcifier: &'x mut rtcifier::RTCifier) -> Self::GenIter {
		match self {
			Self::Status(msg) => Self::GenIter::wrap(msg.readouts(rtcifier)),
			Self::ReadoutData(msg) => msg.readouts(rtcifier),
			Self::StreamData(msg) => msg.readouts(rtcifier),
		}
	}
}
