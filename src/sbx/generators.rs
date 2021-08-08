use std::fmt::Write;

use smartstring::alias::{String as SmartString};

use crate::metric;
use super::frame;
use super::rtcifier;
use super::bme280;

use chrono::{DateTime, Utc};

static MAIN_COMPONENT: &'static str = "main";
pub static DS18B20_TEMP_COMPONENT: &'static str = MAIN_COMPONENT;
pub static BME280_TEMP_COMPONENT: &'static str = "temperature";
pub static BME280_PRESSURE_COMPONENT: &'static str = "pressure";
pub static BME280_HUMIDITY_COMPONENT: &'static str = "humidity";
pub static NOISE_MIN_COMPONENT: &'static str = "min";
pub static NOISE_MAX_COMPONENT: &'static str = "max";
pub static NOISE_RMS_COMPONENT: &'static str = "rms";
pub static LIGHT_RED_COMPONENT: &'static str = "red";
pub static LIGHT_GREEN_COMPONENT: &'static str = "green";
pub static LIGHT_BLUE_COMPONENT: &'static str = "blue";
pub static LIGHT_CLEAR_COMPONENT: &'static str = "clear";

pub struct Empty();

pub trait ReadoutIterable<'x, T: rtcifier::RTCifier> {
	type GenIter: Iterator<Item = metric::Readout>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter;
}

impl Iterator for Empty {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		None
	}
}

pub struct DynSampleIterator<'x>(Box<dyn Iterator<Item = metric::Readout> + 'x>);

impl<'x> Iterator for DynSampleIterator<'x> {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.as_mut().next()
	}
}

impl<'x> DynSampleIterator<'x> {
	pub fn wrap<T: Iterator<Item = metric::Readout> + 'x>(other: T) -> DynSampleIterator<'x> {
		DynSampleIterator(Box::new(other))
	}
}

pub struct DS18B20Readouts<'x, T: rtcifier::RTCifier> {
	rtc: &'x mut T,
	src: &'x frame::SbxDS18B20Message,
	at: usize,
}

impl<'x, T: rtcifier::RTCifier> DS18B20Readouts<'x, T> {
	fn from_msg(msg: &'x frame::SbxDS18B20Message, rtcifier: &'x mut T) -> Self {
		Self{
			rtc: rtcifier,
			src: msg,
			at: 0,
		}
	}
}

impl<'x, T: rtcifier::RTCifier> Iterator for DS18B20Readouts<'x, T> {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		let samples = &self.src.samples;
		if self.at >= samples.len() {
			return None
		}

		let sample = &samples[self.at];
		self.at += 1;
		let mut components = metric::OrderedVec::new();
		components.insert(DS18B20_TEMP_COMPONENT.into(), metric::Value{
			magnitude: (sample.raw_value as f64) / 16.0f64,
			unit: metric::Unit::Celsius,
		});
		let mut instance = SmartString::new();
		instance.push_str("1w/");
		for octet in sample.id.0.iter() {
			write!(instance, "{:02x}", octet).unwrap();
		}
		Some(metric::Readout{
			timestamp: self.rtc.map_to_rtc(self.src.timestamp),
			path: metric::DevicePath{
				device_type: "ds18b20".into(),
				instance,
			},
			components,
		})
	}
}

impl<'x, T: rtcifier::RTCifier + 'static> ReadoutIterable<'x, T> for frame::SbxDS18B20Message {
	type GenIter = DS18B20Readouts<'x, T>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		DS18B20Readouts::from_msg(self, rtcifier)
	}
}

pub struct BME280Readouts(Option<metric::Readout>);

impl BME280Readouts {
	fn from_msg(msg: &frame::SbxBME280Message, rtcifier: &mut impl rtcifier::RTCifier) -> BME280Readouts {
		let mut path: SmartString = "i2c-2/".into();
		write!(path, "{:02x}", 0x76 | (msg.instance & 0x1)).expect("formatting");

		let calibration = bme280::CalibrationData::from_registers(&msg.dig88[..], &msg.dige1[..]);
		let readout = bme280::Readout::from_registers(&msg.readout[..]);
		let ts = rtcifier.map_to_rtc(msg.timestamp);
		#[allow(non_snake_case)]
		let (T, P, H) = readout.decode(&calibration);
		let mut components = metric::OrderedVec::new();
		components.insert(BME280_TEMP_COMPONENT.into(), metric::Value{
			magnitude: (T as f64) / 100.0f64,
			unit: metric::Unit::Celsius,
		});
		components.insert(BME280_PRESSURE_COMPONENT.into(), metric::Value{
			magnitude: (P as f64) / 256.0f64,
			unit: metric::Unit::Pascal,
		});
		components.insert(BME280_HUMIDITY_COMPONENT.into(), metric::Value{
			magnitude: (H as f64) / 1024.0f64,
			unit: metric::Unit::Percent,
		});

		BME280Readouts(Some(metric::Readout{
			timestamp: ts,
			path: metric::DevicePath{
				instance: path,
				device_type: "bme280".into(),
			},
			components: components,
		}))
	}
}

impl Iterator for BME280Readouts {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		let mut result = None;
		std::mem::swap(&mut result, &mut self.0);
		result
	}
}

impl<'x, T: rtcifier::RTCifier> ReadoutIterable<'x, T> for frame::SbxBME280Message {
	type GenIter = BME280Readouts;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		BME280Readouts::from_msg(self, rtcifier)
	}
}

pub struct NoiseReadouts<'x, T: rtcifier::RTCifier> {
	rtc: &'x mut T,
	src: &'x frame::SbxNoiseMessage,
	at: usize,
}

impl<'x, T: rtcifier::RTCifier> NoiseReadouts<'x, T> {
	fn from_msg(msg: &'x frame::SbxNoiseMessage, rtcifier: &'x mut T) -> Self {
		Self{
			rtc: rtcifier,
			src: msg,
			at: 0,
		}
	}
}

impl<'x, T: rtcifier::RTCifier> Iterator for NoiseReadouts<'x, T> {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		let samples = &self.src.samples[..];
		if self.at >= samples.len() {
			return None
		}

		let sample = &samples[self.at];
		self.at += 1;
		let mut components = metric::OrderedVec::new();
		components.insert(NOISE_MIN_COMPONENT.into(), metric::Value{
			magnitude: sample.min as f64,
			unit: metric::Unit::Arbitrary,
		});
		components.insert(NOISE_MAX_COMPONENT.into(), metric::Value{
			magnitude: sample.max as f64,
			unit: metric::Unit::Arbitrary,
		});
		components.insert(NOISE_RMS_COMPONENT.into(), metric::Value{
			magnitude: (sample.sqavg as f64).sqrt(),
			unit: metric::Unit::Arbitrary,
		});
		let instance = "ch-0".into();
		Some(metric::Readout{
			timestamp: self.rtc.map_to_rtc(sample.timestamp),
			path: metric::DevicePath{
				device_type: "mic-preamp".into(),
				instance,
			},
			components,
		})
	}
}

impl<'x, T: rtcifier::RTCifier + 'static> ReadoutIterable<'x, T> for frame::SbxNoiseMessage {
	type GenIter = NoiseReadouts<'x, T>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		NoiseReadouts::from_msg(self, rtcifier)
	}
}

pub struct LightReadouts<'x, T: rtcifier::RTCifier> {
	rtc: &'x mut T,
	src: &'x frame::SbxLightMessage,
	at: usize,
}

impl<'x, T: rtcifier::RTCifier> LightReadouts<'x, T> {
	fn from_msg(msg: &'x frame::SbxLightMessage, rtcifier: &'x mut T) -> Self {
		Self{
			rtc: rtcifier,
			src: msg,
			at: 0,
		}
	}
}

impl<'x, T: rtcifier::RTCifier> Iterator for LightReadouts<'x, T> {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		let samples = &self.src.samples[..];
		if self.at >= samples.len() {
			return None
		}

		let sample = &samples[self.at];
		self.at += 1;
		let mut components = metric::OrderedVec::new();
		components.insert(LIGHT_RED_COMPONENT.into(), metric::Value{
			magnitude: sample.ch[0] as f64,
			unit: metric::Unit::Arbitrary,
		});
		components.insert(LIGHT_GREEN_COMPONENT.into(), metric::Value{
			magnitude: sample.ch[1] as f64,
			unit: metric::Unit::Arbitrary,
		});
		components.insert(LIGHT_BLUE_COMPONENT.into(), metric::Value{
			magnitude: sample.ch[2] as f64,
			unit: metric::Unit::Arbitrary,
		});
		components.insert(LIGHT_CLEAR_COMPONENT.into(), metric::Value{
			magnitude: sample.ch[3] as f64,
			unit: metric::Unit::Arbitrary,
		});
		let instance = "ch-0".into();
		Some(metric::Readout{
			timestamp: self.rtc.map_to_rtc(sample.timestamp),
			path: metric::DevicePath{
				device_type: "tcs3200".into(),
				instance,
			},
			components,
		})
	}
}

impl<'x, T: rtcifier::RTCifier + 'static> ReadoutIterable<'x, T> for frame::SbxLightMessage {
	type GenIter = LightReadouts<'x, T>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		LightReadouts::from_msg(self, rtcifier)
	}
}

enum StatusPart {
	I2CMetrics(u8),
	BME280Metrics(u8),
	TxBufferMetrics,
	CpuMetrics,
	Eof,
}

pub struct StatusReadouts<'x> {
	ts: DateTime<Utc>,
	src: &'x frame::SbxStatusMessage,
	at: StatusPart,
}

impl<'x> StatusReadouts<'x> {
	fn from_msg(msg: &'x frame::SbxStatusMessage, rtcifier: &'x mut impl rtcifier::RTCifier) -> StatusReadouts<'x> {
		StatusReadouts{
			ts: rtcifier.map_to_rtc(msg.uptime),
			src: msg,
			at: StatusPart::I2CMetrics(0),
		}
	}
}

fn cpu_task_name(task: u8) -> &'static str {
	match task {
		0x00 => "idle",
		0x01 => "intr_usart1",
		0x02 => "intr_usart2",
		0x03 => "intr_usart3",
		0x04 => "intr_i2c1",
		0x05 => "intr_i2c2",
		0x06 => "intr_adc",
		0x08 => "scheduler",
		0x09 => "intr_usart1_dma",
		0x0a => "intr_usart2_dma",
		0x0b => "intr_usart3_dma",
		0x0c => "intr_i2c1_dma",
		0x0d => "intr_i2c2_dma",
		0x0e => "intr_adc_dma",
		0x10 => "tx",
		0x11 => "blink",
		0x12 => "stream_accel_x",
		0x13 => "stream_accel_y",
		0x14 => "stream_accel_z",
		0x15 => "stream_compass_x",
		0x16 => "stream_compass_y",
		0x17 => "stream_compass_z",
		0x18 => "light",
		0x19 => "misc",
		0x1a => "onewire",
		0x1b => "noise",
		0x1c => "bme280",
		_ => "",
	}
}

impl<'x> Iterator for StatusReadouts<'x> {
	type Item = metric::Readout;

	fn next(&mut self) -> Option<Self::Item> {
		use StatusPart::*;

		let (next_state, result) = match self.at {
			I2CMetrics(ch) => {
				let mut path: SmartString = "i2c-".into();
				write!(&mut path, "{}", ch+1).unwrap();
				let mut components = metric::OrderedVec::new();
				components.insert(
					"txn_overruns".into(),
					metric::Value{
						magnitude: self.src.i2c_metrics[ch as usize].transaction_overruns as f64,
						unit: metric::Unit::Total,
					},
				);

				let readout = metric::Readout{
					timestamp: self.ts.clone(),
					path: metric::DevicePath{
						instance: path,
						device_type: "driver".into(),
					},
					components,
				};

				(
					if ch == 0 {
						I2CMetrics(1)
					} else {
						BME280Metrics(0)
					},
					Some(readout),
				)
			},
			BME280Metrics(ch) => {
				let mut path: SmartString = "i2c-2/".into();
				write!(&mut path, "{:02x}", 0x76 | ch).unwrap();
				let mut components = metric::OrderedVec::new();
				components.insert(
					"configure".into(),
					metric::Value{
						magnitude: self.src.bme280_metrics[ch as usize].configure_status as f64,
						unit: metric::Unit::Status,
					},
				);
				components.insert(
					"timeouts".into(),
					metric::Value{
						magnitude: self.src.bme280_metrics[ch as usize].timeouts as f64,
						unit: metric::Unit::Total,
					},
				);

				let readout = metric::Readout{
					timestamp: self.ts.clone(),
					path: metric::DevicePath{
						instance: path,
						device_type: "driver".into(),
					},
					components,
				};

				(
					if ch == 0 {
						BME280Metrics(1)
					} else {
						TxBufferMetrics
					},
					Some(readout),
				)
			},
			TxBufferMetrics => {
				let path: SmartString = "tx/buffers".into();
				let mut components = metric::OrderedVec::new();
				components.insert(
					"most_allocated_buffers".into(),
					metric::Value{
						magnitude: self.src.tx_buffer_metrics.most_allocated as f64,
						unit: metric::Unit::Arbitrary,
					},
				);
				components.insert(
					"allocated_buffers".into(),
					metric::Value{
						magnitude: self.src.tx_buffer_metrics.allocated as f64,
						unit: metric::Unit::Arbitrary,
					},
				);
				components.insert(
					"ready_buffers".into(),
					metric::Value{
						magnitude: self.src.tx_buffer_metrics.ready as f64,
						unit: metric::Unit::Arbitrary,
					},
				);
				components.insert(
					"total_buffers".into(),
					metric::Value{
						magnitude: self.src.tx_buffer_metrics.total as f64,
						unit: metric::Unit::Arbitrary,
					},
				);

				(
					CpuMetrics,
					Some(metric::Readout{
						timestamp: self.ts.clone(),
						path: metric::DevicePath{
							instance: path,
							device_type: "kernel".into(),
						},
						components,
					}),
				)
			},
			CpuMetrics => {
				let path: SmartString = "cpu-0".into();
				let mut components = metric::OrderedVec::new();

				for (i, ticks) in self.src.cpu_samples.iter().enumerate() {
					let label = cpu_task_name(i as u8);
					if label == "" {
						continue
					}
					components.insert(
						label.into(),
						metric::Value{
							unit: metric::Unit::Total,
							magnitude: *ticks as f64,
						},
					);
				}

				(
					Eof,
					Some(metric::Readout{
						timestamp: self.ts.clone(),
						path: metric::DevicePath{
							instance: path,
							device_type: "kernel".into(),
						},
						components,
					}),
				)
			},
			Eof => (Eof, None),
		};
		self.at = next_state;
		result
	}
}

impl<'x, T: rtcifier::RTCifier> ReadoutIterable<'x, T> for frame::SbxStatusMessage {
	type GenIter = StatusReadouts<'x>;

	fn readouts(&'x self, rtcifier: &'x mut T) -> Self::GenIter {
		StatusReadouts::from_msg(self, rtcifier)
	}
}


#[cfg(test)]
mod test_ds18b20readouts {
	use super::*;

	use chrono::{Utc, TimeZone, DateTime, Duration};

	use crate::sbx::rtcifier::RTCifier;

	fn new_rtc() -> (rtcifier::LinearRTC, DateTime<Utc>) {
		let dt0 = Utc.ymd(2021, 7, 17).and_hms(16, 28, 0);
		let mut rtc = rtcifier::LinearRTC::default();
		rtc.align(dt0, 0);
		(rtc, dt0)
	}

	#[test]
	fn test_emits_metrics() {
		let (mut rtc, dt0) = new_rtc();
		let msg = frame::SbxDS18B20Message{
			timestamp: 1000,
			samples: vec![
				frame::DS18B20Sample{
					id: frame::DS18B20Id([0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8]),
					raw_value: 0x0263,
				}
			],
		};

		let mut iter = DS18B20Readouts::from_msg(&msg, &mut rtc);
		let item = iter.next().unwrap();
		assert_eq!(item.timestamp, dt0 + Duration::seconds(1));
		assert_eq!(item.path.device_type, "ds18b20");
		assert_eq!(item.path.instance, "1w/0001020304050607");
		assert_eq!(item.components.get(DS18B20_TEMP_COMPONENT).unwrap().magnitude, 38.1875f64);
		assert_eq!(item.components.get(DS18B20_TEMP_COMPONENT).unwrap().unit, metric::Unit::Celsius);

		assert!(iter.next().is_none());
	}

	#[test]
	fn test_emits_metrics_for_multiple_ids() {
		let (mut rtc, dt0) = new_rtc();
		let msg = frame::SbxDS18B20Message{
			timestamp: 1000,
			samples: vec![
				frame::DS18B20Sample{
					id: frame::DS18B20Id([0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8]),
					raw_value: 0x0263,
				},
				frame::DS18B20Sample{
					id: frame::DS18B20Id([
						0x10u8, 0x11u8, 0x12u8, 0x13u8,
						0x14u8, 0x15u8, 0x16u8, 0x17u8,
					]),
					raw_value: -0x0132,
				}
			],
		};

		let mut iter = DS18B20Readouts::from_msg(&msg, &mut rtc);
		let item = iter.next().unwrap();
		assert_eq!(item.timestamp, dt0 + Duration::seconds(1));
		assert_eq!(item.path.device_type, "ds18b20");
		assert_eq!(item.path.instance, "1w/0001020304050607");
		assert_eq!(item.components.get(DS18B20_TEMP_COMPONENT).unwrap().magnitude, 38.1875f64);
		assert_eq!(item.components.get(DS18B20_TEMP_COMPONENT).unwrap().unit, metric::Unit::Celsius);

		let item = iter.next().unwrap();
		assert_eq!(item.timestamp, dt0 + Duration::seconds(1));
		assert_eq!(item.path.device_type, "ds18b20");
		assert_eq!(item.path.instance, "1w/1011121314151617");
		assert_eq!(item.components.get(DS18B20_TEMP_COMPONENT).unwrap().magnitude, -19.125f64);
		assert_eq!(item.components.get(DS18B20_TEMP_COMPONENT).unwrap().unit, metric::Unit::Celsius);

		assert!(iter.next().is_none());
	}
}
