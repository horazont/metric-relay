use std::fmt::Write;
use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use log::{debug, trace, warn};

use smartstring::alias::String as SmartString;

use chrono::Utc;

use tokio::sync::broadcast;
use tokio::task::spawn_blocking;

use i2c_linux::I2c;

use crate::bme280;
use crate::metric;

use super::payload;
use super::traits::{null_receiver, Source};

pub struct BME280Worker {
	bus: Arc<Mutex<I2c<File>>>,
	instance: Arc<SmartString>,
	interval: Duration,
	reconfigure_each: usize,
	backoff: Duration,
	calibration: Option<Arc<bme280::CalibrationData>>,
	sink: broadcast::Sender<payload::Sample>,
}

const REG_ID: u8 = 0xd0;
const REG_CONFIG: u8 = 0xf5;
const REG_CTRL_HUM: u8 = 0xf2;
const REG_CTRL_MEAS: u8 = 0xf4;
const REG_DATA_START: u8 = 0xf7;
const REG_DIG88: u8 = 0x88;
const REG_DIGE1: u8 = 0xe1;

const ID: u8 = 0x60;
const DIG88_SIZE: usize = 26;
const DIGE1_SIZE: usize = 7;
const READOUT_SIZE: usize = 8;

const CONFIG: u8 =
	0b0u8 |  // no SPI 3w mode
	(0b001u8 << 2) |  // filter coefficient 2
	(0b101u8 << 5)  // 1000 ms standby time
;

const CTRL_HUM: u8 =
	0b010  // oversample humidity x2
;

const CTRL_MEAS: u8 =
	0b11u8 |  // "normal mode"
	(0b101u8 << 2) |  // oversample pressure x16
	(0b010u8 << 5)  // oversample temperature x2
;

impl BME280Worker {
	pub fn spawn(
		bus: I2c<File>,
		instance: SmartString,
		interval: Duration,
		reconfigure_each: usize,
		sink: broadcast::Sender<payload::Sample>,
	) {
		let mut worker = Self {
			bus: Arc::new(Mutex::new(bus)),
			interval,
			instance: Arc::new(instance),
			reconfigure_each,
			backoff: Duration::from_secs(5),
			calibration: None,
			sink,
		};
		tokio::spawn(async move { worker.run().await });
	}

	fn verified_write<T: AsRawFd>(bus: &mut I2c<T>, reg: u8, data: u8) -> io::Result<()> {
		trace!("writing 0x{:x} to register 0x{:x}", data, reg);
		bus.smbus_write_byte_data(reg, data)?;
		let readback = bus.smbus_read_byte_data(reg)?;
		if readback != data {
			Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!(
					"readback on verified write returned different data: 0x{:x} != 0x{:x}",
					readback, data
				),
			))
		} else {
			Ok(())
		}
	}

	fn reconfigure_blocking<T: AsRawFd>(bus: &mut I2c<T>) -> io::Result<bme280::CalibrationData> {
		let id = bus.smbus_read_byte_data(REG_ID)?;
		trace!("device identified as 0x{:x}", id);
		if id != ID {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"invalid response to ID command",
			));
		}

		debug!(
			"configuring with 0x{:x} 0x{:x} 0x{:x}",
			CONFIG, CTRL_MEAS, CTRL_HUM
		);
		Self::verified_write(bus, REG_CONFIG, CONFIG)?;
		Self::verified_write(bus, REG_CTRL_MEAS, CTRL_MEAS)?;
		Self::verified_write(bus, REG_CTRL_HUM, CTRL_HUM)?;
		trace!("configuration complete");

		let mut dig88_buf = [0u8; DIG88_SIZE];
		let mut dige1_buf = [0u8; DIGE1_SIZE];

		trace!("reading calibration data from 0x88");
		bus.i2c_read_block_data(REG_DIG88, &mut dig88_buf[..])?;
		trace!("reading calibration data from 0xe1");
		bus.i2c_read_block_data(REG_DIGE1, &mut dige1_buf[..])?;

		trace!("parsing calibration data");
		let calibration = bme280::CalibrationData::from_registers(&dig88_buf[..], &dige1_buf[..]);

		trace!("calibration: {:?}", calibration);
		Ok(calibration)
	}

	async fn reconfigure(&mut self) -> io::Result<bme280::CalibrationData> {
		let bus = self.bus.clone();
		match spawn_blocking(move || {
			let mut bus = bus.lock().unwrap();
			Self::reconfigure_blocking(&mut bus)
		})
		.await
		{
			Ok(v) => v,
			Err(e) => Err(io::Error::new(
				io::ErrorKind::Other,
				format!("configuration task panic'd: {}", e),
			)),
		}
	}

	fn sample_blocking<T: AsRawFd>(
		bus: &mut I2c<T>,
		calibration: &bme280::CalibrationData,
		instance: &SmartString,
	) -> io::Result<metric::Readout> {
		let mut data_buf = [0u8; READOUT_SIZE];
		bus.i2c_read_block_data(REG_DATA_START, &mut data_buf[..])?;
		let timestamp = Utc::now();
		trace!("read raw data: {:x?} @ {}", data_buf, timestamp);
		let readout = bme280::Readout::from_registers(&data_buf[..]);
		drop(data_buf);
		trace!("unpacked data: {:?}", readout);
		let (temperature, pressure, humidity) = readout.decodef(calibration);
		trace!(
			"decoded data: {}°C {}Pa {}%rH",
			temperature,
			pressure,
			humidity
		);

		let mut components = metric::OrderedVec::new();
		components.insert(
			bme280::HUMIDITY_COMPONENT.into(),
			metric::Value {
				magnitude: humidity,
				unit: metric::Unit::Percent,
			},
		);
		components.insert(
			bme280::PRESSURE_COMPONENT.into(),
			metric::Value {
				magnitude: pressure,
				unit: metric::Unit::Pascal,
			},
		);
		components.insert(
			bme280::TEMPERATURE_COMPONENT.into(),
			metric::Value {
				magnitude: temperature,
				unit: metric::Unit::Celsius,
			},
		);

		Ok(metric::Readout {
			timestamp,
			path: metric::DevicePath {
				device_type: "bme280".into(),
				instance: instance.clone(),
			},
			components,
		})
	}

	async fn sample(&mut self) -> io::Result<metric::Readout> {
		let bus = self.bus.clone();
		let calibration = self.calibration.as_ref().unwrap().clone();
		let instance = self.instance.clone();
		match spawn_blocking(move || {
			let mut bus = bus.lock().unwrap();
			Self::sample_blocking(&mut bus, &calibration, &instance)
		})
		.await
		{
			Ok(v) => v,
			Err(e) => Err(io::Error::new(
				io::ErrorKind::Other,
				format!("sample task panic'd: {}", e),
			)),
		}
	}

	async fn run(&mut self) {
		let mut next_sample = Instant::now();
		let mut reconfigure_ctr = 0;
		loop {
			if reconfigure_ctr == self.reconfigure_each || self.calibration.is_none() {
				self.calibration = match self.reconfigure().await {
					Ok(v) => Some(Arc::new(v)),
					Err(e) => {
						warn!(
							"failed to reconfigure BME280 ({}), re-trying in {:?}",
							e, self.backoff
						);
						tokio::time::sleep(self.backoff).await;
						continue;
					}
				};
				reconfigure_ctr = 0;
			}

			let now = Instant::now();
			trace!("now = {:?}  next_sample = {:?}", now, next_sample);
			if let Some(sleep_time) = next_sample.checked_duration_since(now) {
				trace!("sleeping {:?} before next sample", sleep_time);
				tokio::time::sleep(sleep_time).await;
			} else {
				trace!(
					"I am a bit late for taking the sample ({:?})",
					now.checked_duration_since(next_sample).unwrap()
				);
			}

			let readout = match self.sample().await {
				Ok(v) => v,
				Err(e) => {
					warn!(
						"failed to read BME280 sample: {}; will attempt reconfiguration next",
						e
					);
					reconfigure_ctr = self.reconfigure_each;
					continue;
				}
			};

			match self.sink.send(vec![Arc::new(readout)]) {
				Ok(_) => (),
				Err(_) => {
					warn!("lost BME280 sample because nobody wanted to have it");
				}
			}

			// If we are always on time, this means we'll keep a steady rhythm.
			// If, however, we are late for a sample, we'll take the next sample later instead of trying to catch up (trying to catch up makes no sense because the changes from in between cannot be recovered anyway).
			// If we take longer to sample than the interval tells us we may, we'll be sampling continuously, but that's ok, too.
			let accurate_next_sample = next_sample + self.interval;
			let fallback_next_sample = now + self.interval;
			let now = Instant::now();
			next_sample = match now.checked_duration_since(accurate_next_sample) {
				Some(tardiness) => {
					trace!("using fallback instant {:?} instead of accurate {:?}, because at {:?} I am already late by {:?}", fallback_next_sample, accurate_next_sample, now, tardiness);
					fallback_next_sample
				}
				None => {
					trace!(
						"using accurate next sample timestamp {:?} because at {:?} I am on time",
						accurate_next_sample,
						now
					);
					accurate_next_sample
				}
			};
			reconfigure_ctr += 1;
		}
	}
}

pub struct BME280 {
	zygote: broadcast::Sender<payload::Sample>,
}

impl BME280 {
	pub fn new<P: AsRef<Path>>(
		bus_device: P,
		address: u8,
		path_prefix: String,
		interval: Duration,
		reconfigure_each: usize,
	) -> io::Result<BME280> {
		let bus_device = bus_device.as_ref();
		let mut bus = I2c::from_path(bus_device)?;
		bus.smbus_set_slave_address(address as u16, false)?;
		let mut instance = SmartString::new();
		instance.push_str(&path_prefix);
		instance.push_str("/");
		instance.push_str(bus_device.file_name().unwrap().to_string_lossy().as_ref());
		write!(instance, "/{:x}", address).unwrap();
		let (zygote, _) = broadcast::channel(8);
		BME280Worker::spawn(bus, instance, interval, reconfigure_each, zygote.clone());
		Ok(Self { zygote })
	}
}

impl Source for BME280 {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}
