use log::{warn};

use tokio::sync::mpsc;

use crate::influxdb;

use super::traits;
use super::payload;
use super::adapter::Serializer;

struct InfluxDBWorker {
	client: influxdb::Client,
	samples: mpsc::Receiver<payload::Sample>,
	database: String,
	retention_policy: Option<String>,
	precision: influxdb::Precision,
}

impl InfluxDBWorker {
	async fn run(&mut self) {
		loop {
			let sample = match self.samples.recv().await {
				None => return,
				Some(v) => v,
			};
			match self.client.post(
				&self.database,
				self.retention_policy.as_ref().and_then(|x| { Some(&x[..]) }),
				self.precision,
				None,
				&sample).await
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
			precision: influxdb::Precision) -> Self {
		let (serializer, samples) = Serializer::new(32);
		let mut worker = InfluxDBWorker{
			client: influxdb::Client::new(
				api_url,
				auth,
			),
			samples,
			database,
			retention_policy,
			precision,
		};
		tokio::spawn(async move {
			worker.run().await
		});
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
