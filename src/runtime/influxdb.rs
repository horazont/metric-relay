use std::sync::Arc;

use log::warn;

use tokio::sync::mpsc;

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
			let mut maybe_readouts = Some(match self.samples.recv().await {
				None => return,
				Some(v) => v,
			});

			let mut by_precision = enum_map::enum_map! {
				_ => Vec::new(),
			};
			// instead of receiving just once, we'll try to get more and more
			// samples until the queue is empty. as we don't yield, we're
			// likely to be much faster consuming than anyone can be
			// producing, or at least I hope so ... ok, you got me, let's
			// build in a handbreak. But other than that, the idea is that
			// influx is writing to disk and that may be slow and if we only
			// submit stuff one by one we'll easily be too slow for the
			// serializer.

			// This is the promised handbreak: we'll only batch up to 256
			// readouts. This is unlikely to ever happen because serializers
			// are generally less deep than that.
			for _ in 0..256 {
				let mut readouts = match maybe_readouts {
					Some(v) => v,
					None => continue,
				};
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
					let target: &mut Vec<Arc<influxdb::Readout>> =
						&mut by_precision[influx_readout.precision];
					if target.capacity() == 0 {
						// optimize for the general case where all samples will have the same precision
						target.reserve(nreadouts);
					}
					target.push(influx_readout);
				}
				// if it fails, we don't care why -- we'll see it when we call
				// recv() the next time and handle it there.
				maybe_readouts = self.samples.try_recv().ok();
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
