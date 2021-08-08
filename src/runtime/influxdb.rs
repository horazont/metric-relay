use std::sync::Arc;

use log::{warn};

use tokio::sync::mpsc;

use crate::influxdb;
use crate::influxdb::Filter;

use super::traits;
use super::payload;
use super::adapter::Serializer;

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
			)
	{
		let mut worker = Self{
			client,
			samples,
			database,
			retention_policy,
			precision,
			filters,
		};
		tokio::spawn(async move {
			worker.run().await
		});
	}

	async fn run(&mut self) {
		loop {
			let mr_readout = match self.samples.recv().await {
				None => return,
				Some(v) => v,
			};

			let influx_readout = match self.filters.process(Arc::new(
				influxdb::Readout::from_metric(&mr_readout, self.precision)
			)) {
				Some(v) => v,
				None => continue,
			};

			match self.client.post(
				&self.database,
				self.retention_policy.as_ref().and_then(|x| { Some(&x[..]) }),
				None,
				&influx_readout).await
			{
				Ok(_) => (),
				Err(e) => warn!("lost sample: failed to submit to influxdb: {}", e),
			};
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
			filters: Vec<Box<dyn Filter>>) -> Self {
		let (serializer, samples) = Serializer::new(128);
		InfluxDBWorker::spawn(
			influxdb::Client::new(
				api_url,
				auth,
			),
			samples,
			database,
			retention_policy,
			precision,
			filters,
		);
		Self{
			samples: serializer,
		}
	}
}

impl traits::Sink for InfluxDBSink {
	fn attach_source<'x>(&self, src: &'x dyn traits::Source) {
		self.samples.attach(src.subscribe_to_samples())
	}
}
