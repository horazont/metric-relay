use std::borrow::Borrow;
use std::sync::Arc;
use std::collections::HashMap;

use log::{warn, info, trace};

use tokio::sync::broadcast;
use tokio::sync::mpsc;

mod payload;
mod traits;
mod config;
mod sbx;
mod adapter;
mod debug;
mod filter;
mod relay;

use adapter::Serializer;

pub use traits::{Source, Sink, null_receiver};
pub use config::{Config, BuildError};

use filter::Filter;

pub struct SampleRouter {
	serializer: Serializer<payload::Sample>,
	sink: broadcast::Sender<payload::Sample>,
}

impl SampleRouter {
	pub fn new(filters: Vec<Box<dyn filter::Filter>>) -> Self {
		let (sink, _) = broadcast::channel(8);
		let (serializer, source) = Serializer::new(8);
		let result = Self{
			serializer,
			sink,
		};
		result.spawn_into_background(source, filters);
		result
	}

	fn spawn_into_background(
			&self,
			mut source: mpsc::Receiver<payload::Sample>,
			filters: Vec<Box<dyn filter::Filter>>) {
		let sink = self.sink.clone();
		tokio::spawn(async move {
			loop {
				let mut item = match source.recv().await {
					Some(item) => item,
					// channel closed, which means that the serializer dropped, which means that the router itself was dropped, which means we can also just go home now.
					None => return,
				};
				if filters.len() > 0 {
					let buffer = match filters.process((*item).clone()) {
						Some(new) => new,
						None => {
							trace!("readout got dropped by filter");
							continue;
						},
					};
					let raw_item = Arc::make_mut(&mut item);
					*raw_item = buffer;
				}
				match sink.send(item) {
					Ok(_) => (),
					Err(_) => {
						warn!("no receivers on route, dropping item");
						continue;
					}
				}
			}
		});
	}
}

impl Source for SampleRouter {
	fn subscribe_to_samples(&self) -> broadcast::Receiver<payload::Sample> {
		self.sink.subscribe()
	}

	fn subscribe_to_streams(&self) -> broadcast::Receiver<payload::Stream> {
		null_receiver()
	}
}

impl Sink for SampleRouter {
	fn attach_source<'x>(&mut self, src: &'x dyn Source) {
		self.serializer.attach(src.subscribe_to_samples())
	}
}

pub struct Runtime {
	#[allow(dead_code)]
	sources: HashMap<String, Box<dyn Source>>,
	#[allow(dead_code)]
	sinks: HashMap<String, Box<dyn Sink>>,
	#[allow(dead_code)]
	routers: Vec<Box<SampleRouter>>,
}

impl Config {
	pub fn check(&self) -> Option<BuildError> {
		for (i, route) in self.routes.iter().enumerate() {
			if !self.sources.contains_key(&route.source) {
				return Some(BuildError::UndefinedSource{
					which: route.source.clone(),
					at: format!("route {}", i+1),
				})
			}

			if !self.sinks.contains_key(&route.sink) {
				return Some(BuildError::UndefinedSink{
					which: route.source.clone(),
					at: format!("route {}", i+1),
				})
			}
		}
		None
	}

	pub fn build(&self) -> Result<Runtime, BuildError> {
		let mut sources: HashMap<String, Box<dyn Source>> = HashMap::new();
		let mut sinks = HashMap::new();
		let mut routers = Vec::new();

		match self.check() {
			Some(e) => return Err(e),
			None => (),
		}

		for (name, ref source_cfg) in self.sources.iter() {
			sources.insert(name.clone(), source_cfg.build()?);
		}

		for (name, ref sink_cfg) in self.sinks.iter() {
			sinks.insert(name.clone(), sink_cfg.build()?);
		}

		for ref router_cfg in self.routes.iter() {
			let mut filters = Vec::new();
			for filter in router_cfg.filters.iter() {
				filters.push(filter.build()?);
			}
			let mut rtr = SampleRouter::new(filters);
			let source = sources.get(&router_cfg.source).expect("lookup source for router");
			let sink = sinks.get_mut(&router_cfg.sink).expect("lookup sink for router");
			rtr.attach_source((*source).borrow());
			sink.attach_source(&rtr);
			routers.push(Box::new(rtr));
		}

		info!("linked up {} sources, {} sinks and {} routers", sources.len(), sinks.len(), routers.len());

		Ok(Runtime{
			sources,
			sinks,
			routers,
		})
	}
}
