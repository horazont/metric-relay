use std::io;
use std::sync::Arc;
use std::time::Duration;

use smartstring::alias::String as SmartString;

use chrono::TimeZone;

use log::{info, warn};

use tokio::sync::broadcast;

use csv;

use crate::metric;

use super::payload;
use super::traits::{null_receiver, Source};

fn try_get(rec: &csv::StringRecord, i: usize) -> io::Result<&str> {
	match rec.get(i) {
		Some(v) => Ok(v),
		None => Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("missing column {} in CSV row", i),
		)),
	}
}

fn decode_value(v: &str) -> io::Result<f64> {
	let value: f64 = match v.parse() {
		Ok(v) => v,
		Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
	};
	Ok(value)
}

fn decode_timestamp(v: &str) -> io::Result<chrono::DateTime<chrono::Utc>> {
	let unixtime_nanos: i64 = match v.parse() {
		Ok(v) => v,
		Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
	};
	Ok(chrono::Utc.timestamp_nanos(unixtime_nanos))
}

struct InjectionWorker {
	instance_index: usize,
	device_type_index: usize,
	timestamp_index: usize,
	start_time: chrono::DateTime<chrono::Utc>,
	end_time: chrono::DateTime<chrono::Utc>,
	offset: chrono::Duration,
	components: Vec<(usize, SmartString, metric::Unit)>,
	sleep: Duration,
	batch: usize,
}

impl InjectionWorker {
	fn unpack(&self, record: csv::StringRecord) -> io::Result<Option<Arc<metric::Readout>>> {
		let timestamp = decode_timestamp(try_get(&record, self.timestamp_index)?)?;
		if timestamp < self.start_time || timestamp >= self.end_time {
			return Ok(None);
		}
		let timestamp = timestamp + self.offset;

		let device_type: SmartString = try_get(&record, self.device_type_index)?.into();
		let instance: SmartString = try_get(&record, self.instance_index)?.into();
		let mut components = metric::OrderedVec::new();
		for (offset, name, unit) in self.components.iter() {
			components.insert(
				name.clone(),
				metric::Value {
					magnitude: decode_value(try_get(&record, *offset)?)?,
					unit: unit.clone(),
				},
			);
		}
		Ok(Some(Arc::new(metric::Readout {
			timestamp,
			path: metric::DevicePath {
				instance,
				device_type,
			},
			components,
		})))
	}

	fn submit_buffer(buf: payload::Sample, sink: &broadcast::Sender<payload::Sample>) {
		match sink.send(buf) {
			Ok(_) => (),
			Err(_) => warn!("no listeners for csv inject samples, dropped"),
		}
	}

	async fn run(
		&mut self,
		mut source: csv::Reader<Box<dyn io::Read + Send + Sync + 'static>>,
		sink: broadcast::Sender<payload::Sample>,
	) {
		// initial sleep to allow things to be set up
		tokio::time::sleep(self.sleep).await;
		let mut buffer = Vec::with_capacity(self.batch);
		for record in source.records() {
			let record = match record {
				Ok(v) => v,
				Err(e) => {
					warn!("failed to read CSV record: {}", e);
					continue;
				}
			};
			buffer.push(match self.unpack(record) {
				Ok(Some(v)) => v,
				// out of range or somesuch
				Ok(None) => continue,
				Err(e) => {
					warn!("invalid record in CSV: {}", e);
					continue;
				}
			});
			if buffer.len() >= self.batch {
				let mut new = Vec::with_capacity(self.batch);
				std::mem::swap(&mut new, &mut buffer);
				Self::submit_buffer(new, &sink);
				tokio::time::sleep(self.sleep).await;
			}
		}
		if buffer.len() > 0 {
			Self::submit_buffer(buffer, &sink);
		}
		info!("end of file in CSV");
	}
}

fn try_find(rec: &csv::StringRecord, name: &str) -> io::Result<usize> {
	for (i, field) in rec.iter().enumerate() {
		if field == name {
			return Ok(i);
		}
	}
	Err(io::Error::new(
		io::ErrorKind::InvalidData,
		format!("no column with title {:?} in CSV", name),
	))
}

pub struct Injector {
	zygote: broadcast::Sender<payload::Sample>,
}

impl Injector {
	pub fn new(
		source: Box<dyn io::Read + Sync + Send + 'static>,
		device_type_column_name: &str,
		instance_column_name: &str,
		timestamp_column_name: &str,
		mut component_mapping: Vec<(&str, SmartString, metric::Unit)>,
		start_time: chrono::DateTime<chrono::Utc>,
		end_time: chrono::DateTime<chrono::Utc>,
		offset: chrono::Duration,
		batch_size: usize,
	) -> io::Result<Injector> {
		let mut reader = csv::ReaderBuilder::default()
			.has_headers(true)
			.from_reader(source);
		let headers = reader.headers()?;
		let device_type_index = try_find(&headers, device_type_column_name)?;
		let instance_index = try_find(&headers, instance_column_name)?;
		let timestamp_index = try_find(&headers, timestamp_column_name)?;
		let mut components = Vec::new();
		for (header, component, unit) in component_mapping.drain(..) {
			components.push((try_find(&headers, header)?, component, unit));
		}
		let mut worker = InjectionWorker {
			device_type_index,
			instance_index,
			timestamp_index,
			start_time,
			end_time,
			offset,
			components,
			batch: batch_size,
			sleep: Duration::from_millis(10),
		};
		let (zygote, _) = broadcast::channel(8);
		let sink = zygote.clone();
		tokio::spawn(async move {
			worker.run(reader, sink).await;
		});
		Ok(Self { zygote })
	}
}

impl Source for Injector {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.zygote.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}
