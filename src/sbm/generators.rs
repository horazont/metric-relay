use std::fmt::Write;

use chrono::{DateTime, Utc};

use smartstring::alias::String as SmartString;

use crate::bme280::{
	HUMIDITY_COMPONENT as BME280_HUMIDITY_COMPONENT,
	PRESSURE_COMPONENT as BME280_PRESSURE_COMPONENT,
	TEMPERATURE_COMPONENT as BME280_TEMP_COMPONENT,
};
use crate::bme68x;
use crate::metric;

use super::frame;

pub struct Empty();

pub trait ReadoutIterable {
	type GenIter: Iterator<Item = metric::Readout>;

	fn readouts(&self, ts: DateTime<Utc>) -> Self::GenIter;
}

impl Iterator for Empty {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		None
	}
}

pub struct DynSampleIterator(Box<dyn Iterator<Item = metric::Readout>>);

impl Iterator for DynSampleIterator {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.as_mut().next()
	}
}

impl DynSampleIterator {
	pub fn wrap<T: Iterator<Item = metric::Readout> + 'static>(other: T) -> DynSampleIterator {
		DynSampleIterator(Box::new(other))
	}
}

pub struct Bme68xReadouts(Option<metric::Readout>);

impl Bme68xReadouts {
	fn from_msg(ts: DateTime<Utc>, msg: &frame::EspBme68xMessage) -> Bme68xReadouts {
		let mut path: SmartString = "i2c-1/".into();
		write!(path, "{:02x}", 0x76 | (msg.instance & 0x1)).expect("formatting");

		let calibration = bme68x::CalibrationData::from_registers(&msg.par8a[..], &msg.pare1[..]);
		let readout = bme68x::Readout::from_registers(&msg.readout[2..]);
		#[allow(non_snake_case)]
		let (T, P, H) = readout.decodef(&calibration);
		let mut components = metric::OrderedVec::new();
		components.insert(
			BME280_TEMP_COMPONENT.into(),
			metric::Value {
				magnitude: T,
				unit: metric::Unit::Celsius,
			},
		);
		components.insert(
			BME280_PRESSURE_COMPONENT.into(),
			metric::Value {
				magnitude: P,
				unit: metric::Unit::Pascal,
			},
		);
		components.insert(
			BME280_HUMIDITY_COMPONENT.into(),
			metric::Value {
				magnitude: H,
				unit: metric::Unit::Percent,
			},
		);

		Self(Some(metric::Readout {
			timestamp: ts,
			path: metric::DevicePath {
				instance: path,
				device_type: "bme688".into(),
			},
			components: components,
		}))
	}
}

impl Iterator for Bme68xReadouts {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		let mut result = None;
		std::mem::swap(&mut result, &mut self.0);
		result
	}
}

impl ReadoutIterable for frame::EspBme68xMessage {
	type GenIter = Bme68xReadouts;

	fn readouts(&self, ts: DateTime<Utc>) -> Self::GenIter {
		Bme68xReadouts::from_msg(ts, self)
	}
}

pub struct StatusReadouts(Option<metric::Readout>);

impl StatusReadouts {
	fn from_msg(ts: DateTime<Utc>, msg: &frame::EspStatus) -> Self {
		let path: SmartString = "trx".into();

		let mut components = metric::OrderedVec::new();
		components.insert(
			"sent".into(),
			metric::Value {
				magnitude: msg.tx_sent as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"dropped".into(),
			metric::Value {
				magnitude: msg.tx_dropped as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"oom_dropped".into(),
			metric::Value {
				magnitude: msg.tx_oom_dropped as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"error".into(),
			metric::Value {
				magnitude: msg.tx_error as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"retransmitted".into(),
			metric::Value {
				magnitude: msg.tx_retransmitted as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"broadcasts".into(),
			metric::Value {
				magnitude: msg.tx_broadcasts as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"queue_overrun".into(),
			metric::Value {
				magnitude: msg.tx_queue_overrun as f64,
				unit: metric::Unit::Total,
			},
		);
		components.insert(
			"acklocks_needed".into(),
			metric::Value {
				magnitude: msg.tx_acklocks_needed as f64,
				unit: metric::Unit::Total,
			},
		);

		Self(Some(metric::Readout {
			timestamp: ts,
			path: metric::DevicePath {
				instance: path,
				device_type: "esp-tx".into(),
			},
			components: components,
		}))
	}
}

impl Iterator for StatusReadouts {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		let mut result = None;
		std::mem::swap(&mut result, &mut self.0);
		result
	}
}

impl ReadoutIterable for frame::EspStatus {
	type GenIter = StatusReadouts;

	fn readouts(&self, ts: DateTime<Utc>) -> Self::GenIter {
		StatusReadouts::from_msg(ts, self)
	}
}
