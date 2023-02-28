use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use smartstring::alias::String as SmartString;
use std::time::Duration;

#[cfg(feature = "metric-serde")]
use serde_derive::{Deserialize, Serialize};

mod maskedarray;
mod orderedvec;

pub use maskedarray::{MaskedArray, MaskedArrayWriter};
pub use orderedvec::OrderedVec;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
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

	pub fn plausible_range(&self) -> (Option<f64>, Option<f64>) {
		match self {
			Self::Celsius => (Some(-273.15), None),
			Self::Percent => (Some(0.0), Some(100.0)),
			Self::Kelvin => (Some(0.0), None),
			_ => (None, None),
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
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

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
pub struct Value {
	pub magnitude: f64,
	pub unit: Unit,
}

impl fmt::Display for Value {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		let suffix = self.unit.as_str();
		if suffix.len() > 0 {
			write!(f, "{} {}", self.magnitude, suffix)
		} else {
			write!(f, "{}", self.magnitude)
		}
	}
}

impl Value {
	pub fn is_plausible(&self) -> bool {
		let (min, max) = self.unit.plausible_range();
		if let Some(min) = min {
			if self.magnitude < min {
				return false;
			}
		}
		if let Some(max) = max {
			if self.magnitude > max {
				return false;
			}
		}
		true
	}
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
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
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
pub enum StreamFormat {
	/* I8,
	U8, */
	I16,
	/* U16,
	I32,
	U32,
	I64,
	U64,
	F32, */
	F64,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
pub enum RawData {
	/* I8(MaskedArray<i8>),
	U8(MaskedArray<u8>), */
	I16(MaskedArray<i16>),
	/* U16(MaskedArray<u16>),
	I32(MaskedArray<i32>),
	U32(MaskedArray<u32>),
	I64(MaskedArray<i64>),
	U64(MaskedArray<u64>),
	F32(MaskedArray<f32>), */
	F64(MaskedArray<f64>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum RawSample {
	/* I8(MaskedArray<i8>),
	U8(MaskedArray<u8>), */
	I16(Option<i16>),
	/* U16(MaskedArray<u16>),
	I32(MaskedArray<i32>),
	U32(MaskedArray<u32>),
	I64(MaskedArray<i64>),
	U64(MaskedArray<u64>),
	F32(MaskedArray<f32>), */
	F64(Option<f64>),
}

impl RawSample {
	pub fn normalized(self) -> Option<f64> {
		match self {
			Self::I16(v) => Some((v? as f64) / (i16::MAX as f64)),
			Self::F64(v) => Some(v?),
		}
	}
}

impl RawData {
	pub fn len(&self) -> usize {
		match self {
			Self::I16(v) => v.len(),
			Self::F64(v) => v.len(),
		}
	}

	pub fn iter(&self) -> RawDataIter<'_> {
		match self {
			Self::I16(a) => RawDataIter::I16(a.iter_optional(..)),
			Self::F64(a) => RawDataIter::F64(a.iter_optional(..)),
		}
	}
}

pub enum RawDataIter<'x> {
	I16(maskedarray::Optional<'x, i16>),
	F64(maskedarray::Optional<'x, f64>),
}

impl<'x> Iterator for RawDataIter<'x> {
	type Item = RawSample;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			Self::I16(ref mut iter) => Some(RawSample::I16(iter.next()?.copied())),
			Self::F64(ref mut iter) => Some(RawSample::F64(iter.next()?.copied())),
		}
	}
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "metric-serde", derive(Serialize, Deserialize))]
pub struct StreamBlock {
	pub t0: DateTime<Utc>,
	pub path: DevicePath,
	pub seq0: u16,
	pub period: Duration,
	pub scale: Value,
	pub data: Arc<RawData>,
}
