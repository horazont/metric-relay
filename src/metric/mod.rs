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

	// raw decibel
	DeciBel,
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
			Self::DeciBel => "dB",
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
			"dB" => Ok(Self::DeciBel),
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

impl fmt::Display for DevicePath {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", self.device_type, self.instance)
	}
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

impl RawData {
	pub fn len(&self) -> usize {
		match self {
			Self::I16(v) => v.len(),
		}
	}
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StreamBlock {
	pub t0: DateTime<Utc>,
	pub path: DevicePath,
	pub seq0: u16,
	pub period: Duration,
	pub scale: Value,
	pub data: RawData,
}
