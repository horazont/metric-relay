use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{error, trace, warn};

use smartstring::alias::String as SmartString;

use serde_derive::Deserialize;

use chrono::Utc;

use tokio::sync::broadcast;

use crate::metric;

use super::payload;
use super::traits::{null_receiver, Source};

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Type {
	TempInput,
}

impl Type {
	fn file_name(&self, sensor: u32) -> PathBuf {
		match self {
			Self::TempInput => format!("temp{}_input", sensor).into(),
		}
	}

	fn read<P: AsRef<Path>>(&self, path: P) -> io::Result<metric::Value> {
		let value_str = fs::read_to_string(path)?;
		match self {
			Self::TempInput => {
				let value = match u64::from_str(value_str.trim()) {
					Ok(v) => v,
					Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
				};
				Ok(metric::Value {
					unit: metric::Unit::Celsius,
					magnitude: (value as f64) / 1000.,
				})
			}
		}
	}
}

pub struct Sensor {
	// TODO: re-find the thing on I/O errors
	#[allow(dead_code)]
	hwmon_name: String,
	hwmon_path: PathBuf,
	type_: Type,
	sensor: u32,
	component: SmartString,
}

impl Sensor {
	fn find_by_name(name: &str) -> io::Result<PathBuf> {
		let root: &Path = AsRef::<Path>::as_ref("/sys/class/hwmon");
		for entry in fs::read_dir(root)? {
			let entry = match entry {
				Ok(v) => v,
				Err(e) => {
					warn!("failed to read entry from hwmon tree: {}", e);
					continue;
				}
			};
			match entry.file_type() {
				Ok(type_) if type_.is_symlink() => (),
				// if error or not a symlink, skip
				_ => {
					trace!(
						"hwmon entry {:?} is not a symlink or not accessible",
						entry.path()
					);
					continue;
				}
			};
			let hwmon_path = match fs::read_link(entry.path()) {
				Ok(v) => {
					let mut path: PathBuf = root.into();
					path.push(v);
					path
				}
				// whatever this was
				Err(e) => {
					warn!(
						"failed to resolve symlink {:?} in hwmon tree: {}",
						entry.path(),
						e
					);
					continue;
				}
			};
			let mut name_path = hwmon_path.clone();
			name_path.push("name");
			match fs::read_to_string(&name_path) {
				Ok(v) => {
					if v.trim() == name {
						return Ok(hwmon_path);
					}
				}
				Err(e) => {
					warn!("failed to read name of hwmon at {:?}: {}", name_path, e);
					continue;
				}
			}
		}
		Err(io::Error::new(
			io::ErrorKind::NotFound,
			format!("no hwmon with name {:?} found", name),
		))
	}

	pub fn new(name: &str, sensor: u32, type_: Type, component: SmartString) -> io::Result<Self> {
		let hwmon_path = Self::find_by_name(name)?;
		let mut sensor_path = hwmon_path.clone();
		sensor_path.push(type_.file_name(sensor));
		// the input file must exist
		fs::metadata(sensor_path)?;
		Ok(Self {
			hwmon_name: name.into(),
			hwmon_path,
			sensor,
			type_,
			component,
		})
	}

	fn sample(&self) -> io::Result<metric::Value> {
		let mut sensor_path = self.hwmon_path.clone();
		sensor_path.push(self.type_.file_name(self.sensor));
		self.type_.read(sensor_path)
	}
}

pub struct Scrape {
	interval: Duration,
	path: metric::DevicePath,
	sensors: Vec<Sensor>,
}

impl Scrape {
	pub fn new(interval: Duration, path: metric::DevicePath, sensors: Vec<Sensor>) -> Self {
		Self {
			interval,
			path,
			sensors,
		}
	}

	fn sample(&self) -> io::Result<payload::Sample> {
		let mut readout = metric::Readout {
			timestamp: Utc::now(),
			path: self.path.clone(),
			components: metric::OrderedVec::new(),
		};
		for sensor in self.sensors.iter() {
			readout
				.components
				.insert(sensor.component.clone(), sensor.sample()?);
		}
		Ok(vec![Arc::new(readout)])
	}

	async fn run(&self, sink: broadcast::Sender<payload::Sample>) {
		let mut next = Instant::now();
		loop {
			let now = Instant::now();
			match next.checked_duration_since(now) {
				Some(to_sleep) => {
					tokio::time::sleep(to_sleep).await;
				}
				None => {}
			};
			let sample = match self.sample() {
				Ok(v) => v,
				Err(e) => {
					error!("failed to sample hwmon: {}", e);
					return;
				}
			};
			match sink.send(sample) {
				Ok(_) => (),
				Err(_) => {
					warn!("no receivers on route, dropping scrape");
					continue;
				}
			}
			let now = Instant::now();
			// skip samples if we're too slow, but stay true to the rhythm
			let factor = match now.checked_duration_since(next) {
				Some(v) => (v.as_micros() / self.interval.as_micros() + 1) as u32,
				None => 1,
			};
			next = next + self.interval * factor;
		}
	}
}

pub struct Hwmon {
	zygote: broadcast::Sender<payload::Sample>,
}

impl Hwmon {
	pub fn new(scrape: Scrape) -> Self {
		let (zygote, _) = broadcast::channel(8);
		let sink = zygote.clone();
		tokio::spawn(async move {
			scrape.run(sink).await;
		});
		Self { zygote }
	}
}

impl Source for Hwmon {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}
