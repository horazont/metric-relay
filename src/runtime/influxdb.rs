use std::sync::Arc;

use log::warn;

use tokio::sync::mpsc;

use enum_map::EnumMap;

use crate::influxdb;
use crate::influxdb::Filter;

use super::adapter::Serializer;
use super::payload;
use super::traits;

struct InfluxDBWorker {
	client: influxdb::Client,
	samples: mpsc::Receiver<payload::Sample>,
	database: String,
	retention_policy: Option<String>,
	precision: influxdb::Precision,
	filters: Vec<Box<dyn Filter>>,
}

impl InfluxDBWorker {
	pub fn spawn(
		client: influxdb::Client,
		samples: mpsc::Receiver<payload::Sample>,
		database: String,
		retention_policy: Option<String>,
		precision: influxdb::Precision,
		filters: Vec<Box<dyn Filter>>,
	) {
		let mut worker = Self {
			client,
			samples,
			database,
			retention_policy,
			precision,
			filters,
		};
		tokio::spawn(async move { worker.run().await });
	}

	async fn run(&mut self) {
		loop {
			let mut readouts = match self.samples.recv().await {
				None => return,
				Some(v) => v,
			};

			let mut by_precision =
				EnumMap::<influxdb::Precision, Vec<Arc<influxdb::Readout>>>::new();
			let nreadouts = readouts.len();
			for readout in readouts.drain(..) {
				let influx_readout =
					match self
						.filters
						.process(Arc::new(influxdb::Readout::from_metric(
							&readout,
							self.precision,
						))) {
						Some(v) => v,
						None => continue,
					};
				let target = &mut by_precision[influx_readout.precision];
				if target.capacity() == 0 {
					// optimize for the general case where all samples will have the same precision
					target.reserve(nreadouts);
				}
				target.push(influx_readout);
			}

			for (precision, readouts) in by_precision.iter() {
				if readouts.len() == 0 {
					continue;
				}
				match self
					.client
					.post(
						&self.database,
						self.retention_policy.as_ref().and_then(|x| Some(&x[..])),
						None,
						precision,
						&readouts[..],
					)
					.await
				{
					Ok(_) => (),
					Err(e) => warn!("lost sample: failed to submit to influxdb: {}", e),
				};
			}
		}
	}
}

pub struct InfluxDBSink {
	samples: Serializer<payload::Sample>,
}

impl InfluxDBSink {
	pub fn new(
		api_url: String,
		auth: influxdb::Auth,
		database: String,
		retention_policy: Option<String>,
		precision: influxdb::Precision,
		filters: Vec<Box<dyn Filter>>,
	) -> Self {
		let (serializer, samples) = Serializer::new(128);
		InfluxDBWorker::spawn(
			influxdb::Client::new(api_url, auth),
			samples,
			database,
			retention_policy,
			precision,
			filters,
		);
		Self {
			samples: serializer,
		}
	}
}

impl traits::Sink for InfluxDBSink {
	fn attach_source<'x>(&self, src: &'x dyn traits::Source) {
		self.samples.attach(src.subscribe_to_samples())
	}
}
