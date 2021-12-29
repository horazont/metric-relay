use std::io;

use smartstring::alias::String as SmartString;

use serde_derive::{Deserialize, Serialize};

use enum_map::Enum;

use chrono::{DateTime, Utc};

use crate::metric;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Enum)]
pub enum Precision {
	Nanoseconds,
	Microseconds,
	Milliseconds,
	Seconds,
}

impl Precision {
	pub fn value(&self) -> &'static str {
		match self {
			Self::Nanoseconds => "ns",
			Self::Microseconds => "u",
			Self::Milliseconds => "ms",
			Self::Seconds => "s",
		}
	}

	pub fn encode_timestamp<W: io::Write>(&self, w: &mut W, ts: &DateTime<Utc>) -> io::Result<()> {
		// XXX: do something about leap seconds
		match self {
			Self::Seconds => write!(w, "{}", ts.timestamp()),
			Self::Milliseconds => {
				let ms = ts.timestamp_subsec_millis();
				let ms = if ms >= 999 { 999 } else { ms };
				write!(w, "{}{:03}", ts.timestamp(), ms)
			}
			Self::Microseconds => {
				let us = ts.timestamp_subsec_micros();
				let us = if us >= 999_999 { 999_999 } else { us };
				write!(w, "{}{:06}", ts.timestamp(), us)
			}
			Self::Nanoseconds => {
				let ns = ts.timestamp_subsec_nanos();
				let ns = if ns >= 999_999_999 { 999_999_999 } else { ns };
				write!(w, "{}{:09}", ts.timestamp(), ns)
			}
		}
	}
}

#[derive(Debug, Clone)]
pub struct Sample {
	pub tagv: Vec<SmartString>,
	pub fieldv: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct Readout {
	pub ts: DateTime<Utc>,
	pub measurement: SmartString,
	pub precision: Precision,
	pub tags: Vec<SmartString>,
	pub fields: Vec<SmartString>,
	pub samples: Vec<Sample>,
}

impl Readout {
	pub fn from_metric(readout: &metric::Readout, precision: Precision) -> Self {
		let tags = vec!["instance".into()];
		let mut fields = Vec::with_capacity(readout.components.len());
		let mut fieldv = Vec::with_capacity(readout.components.len());
		for (k, v) in readout.components.iter() {
			fields.push(k.clone());
			fieldv.push(v.magnitude);
		}

		let samples = vec![Sample {
			tagv: vec![readout.path.instance.clone()],
			fieldv: fieldv,
		}];
		Readout {
			ts: readout.timestamp,
			measurement: readout.path.device_type.clone(),
			precision,
			tags,
			fields,
			samples,
		}
	}

	pub fn write<W: io::Write>(&self, dest: &mut W) -> io::Result<()> {
		// TODO: some proper escaping :>
		for sample in self.samples.iter() {
			dest.write_all(&self.measurement.as_bytes())?;
			for (k, v) in self.tags.iter().zip(sample.tagv.iter()) {
				write!(dest, ",{}={}", k, v)?;
			}
			let mut first = true;
			for (k, v) in self.fields.iter().zip(sample.fieldv.iter()) {
				write!(dest, "{}{}={:?}", if first { ' ' } else { ',' }, k, v)?;
				first = false;
			}
			dest.write_all(&b" "[..])?;
			self.precision.encode_timestamp(dest, &self.ts)?;
			dest.write_all(&b"\n"[..])?;
		}
		Ok(())
	}
}
