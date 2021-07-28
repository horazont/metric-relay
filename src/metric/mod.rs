use std::fmt;
use std::convert::TryInto;
use std::str::FromStr;

use bytes::Bytes;

use chrono::{DateTime, Utc};
use std::time::Duration;
use smartstring::alias::{String as SmartString};
use byteorder::{ReadBytesExt, LittleEndian};

use serde_derive::{Deserialize, Serialize};

mod orderedvec;

pub use orderedvec::OrderedVec;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum Unit {
	// other
	Arbitrary,
	Percent,
	Status,
	Other(SmartString),

	// counters
	Total,

	// temperature
	Kelvin,
	Celsius,

	// acceleration
	MeterPerSqSecond,

	// magnetic fields
	Tesla,

	// pressure
	Pascal,
}

impl Unit {
	fn as_str(&self) -> &str {
		match self {
			Self::Arbitrary => "",
			Self::Percent => "%",
			Self::Other(s) => &s,
			Self::Kelvin => "K",
			Self::Celsius => "°C",
			Self::MeterPerSqSecond => "m/s²",
			Self::Tesla => "T",
			Self::Pascal => "Pa",
			Self::Total => "",
			Self::Status => "",
		}
	}
}

impl FromStr for Unit {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"%" => Ok(Self::Percent),
			"K" => Ok(Self::Kelvin),
			"°C" => Ok(Self::Celsius),
			"T" => Ok(Self::Tesla),
			"AU" => Ok(Self::Arbitrary),
			"cnt" => Ok(Self::Total),
			"Pa" => Ok(Self::Pascal),
			_ => Err("unknown unit"),
		}
	}
}

impl fmt::Display for Unit {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		f.write_str(self.as_str())
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DevicePath {
	/// Type of device which was read
	// e.g. lsm303d
	pub device_type: SmartString,
	/// Full sensor path to the device which was read
	// e.g. "/sbx/i2c/0x78"
	pub instance: SmartString,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Value {
	pub magnitude: f64,
	pub unit: Unit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Readout {
	/// Timestamp of the readout
	pub timestamp: DateTime<Utc>,
	/// Full device path
	pub path: DevicePath,
	/// Components of the readout
	pub components: OrderedVec<SmartString, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
	pub timestamp: DateTime<Utc>,
	pub path: DevicePath,
	pub component: SmartString,
	pub value: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamFormat {
	/* I8,
	U8, */
	I16,
	/* U16,
	I32,
	U32,
	I64,
	U64,
	F32,
	F64, */
}

#[derive(Clone, Debug, PartialEq)]
pub enum RawData {
	/* I8(Vec<i8>),
	U8(Vec<u8>), */
	I16(Vec<i16>),
	/* U16(Vec<u16>),
	I32(Vec<i32>),
	U32(Vec<u32>),
	I64(Vec<i64>),
	U64(Vec<u64>),
	F32(Vec<f32>),
	F64(Vec<f64>), */
}

#[derive(Clone, Debug)]
pub enum StreamBlockData {
	Compressed(StreamFormat, Bytes),
	Uncompressed(StreamFormat, Bytes),
	Decoded(RawData),
}

impl StreamBlockData {
	/// Ensure that the in-memory representation is decoded.
	fn decoded(self) -> StreamBlockData {
		let (fmt, decompressed) = match self {
			StreamBlockData::Compressed(fmt, gzbytes) => (fmt, gzbytes),
			StreamBlockData::Uncompressed(fmt, bytes) => (fmt, bytes),
			StreamBlockData::Decoded(data) => return StreamBlockData::Decoded(data),
		};
		match fmt {
			StreamFormat::I16 => {
				let samples = decompressed.len() / 2;
				let mut buf = Vec::<i16>::new();
				buf.resize(samples, 0i16);
				(&decompressed[..]).read_i16_into::<LittleEndian>(&mut buf[..]).unwrap();
				StreamBlockData::Decoded(RawData::I16(buf))
			}
		}
	}
}

impl From<Vec<i16>> for StreamBlockData {
	fn from(v: Vec<i16>) -> StreamBlockData {
		StreamBlockData::Decoded(RawData::I16(v))
	}
}

impl TryInto<Vec<i16>> for StreamBlockData {
	type Error = StreamFormat;

	fn try_into(self) -> Result<Vec<i16>, Self::Error> {
		let next = match self {
			StreamBlockData::Compressed(StreamFormat::I16, _) => self.decoded(),
			// StreamBlockData::Compressed(other, _) => return Err(other),
			StreamBlockData::Uncompressed(StreamFormat::I16, _) => self.decoded(),
			// StreamBlockData::Uncompressed(other, _) => return Err(other),
			StreamBlockData::Decoded(RawData::I16(_)) => self,
		};
		if let StreamBlockData::Decoded(RawData::I16(v)) = next {
			Ok(v)
		} else {
			unreachable!();
		}
	}
}

#[allow(dead_code)]
pub struct StreamBlock {
	t0: DateTime<Utc>,
	path: DevicePath,
	seq0: u16,
	period: Duration,
	scale: Value,
	data: StreamBlockData,
}
