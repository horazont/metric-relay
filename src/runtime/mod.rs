use std::collections::HashMap;

mod adapter;
mod config;
#[cfg(feature = "debug")]
mod debug;
#[cfg(feature = "detrend")]
mod detrend;
#[cfg(feature = "fft")]
mod fft;
mod filter;
#[cfg(feature = "influxdb")]
mod influxdb;
mod payload;
#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(feature = "relay")]
mod relay;
#[cfg(feature = "sbx")]
mod retry;
mod router;
#[cfg(feature = "sbx")]
mod sbx;
#[cfg(feature = "smbus")]
mod smbus;
#[cfg(feature = "stream-filearchive")]
mod stream;
#[cfg(feature = "summary")]
mod summary;
mod traits;

pub use config::{BuildError, Config};
pub use traits::{Node, Sink, Source};

pub struct Runtime {
	#[allow(dead_code)]
	nodes: HashMap<String, Node>,
}

impl Config {
	fn get_source<'x>(
		nodes: &'x HashMap<String, Node>,
		name: &'_ str,
	) -> Result<&'x dyn Source, BuildError> {
		match nodes.get(name) {
			None => Err(BuildError::UndefinedSource { which: name.into() }),
			Some(node) => match node.as_source() {
				None => Err(BuildError::NotASource { which: name.into() }),
				Some(src) => Ok(src),
			},
		}
	}

	fn get_sink<'x>(
		nodes: &'x HashMap<String, Node>,
		name: &'_ str,
	) -> Result<&'x dyn Sink, BuildError> {
		match nodes.get(name) {
			None => Err(BuildError::UndefinedSink { which: name.into() }),
			Some(node) => match node.as_sink() {
				None => Err(BuildError::NotASink { which: name.into() }),
				Some(src) => Ok(src),
			},
		}
	}

	pub fn build(&self) -> Result<Runtime, BuildError> {
		let mut nodes = HashMap::new();
		for (name, ref node_cfg) in self.node.iter() {
			nodes.insert(name.clone(), node_cfg.build()?);
		}

		for ref link_cfg in self.link.iter() {
			let src = Self::get_source(&nodes, &link_cfg.source)?;
			let sink = Self::get_sink(&nodes, &link_cfg.sink)?;
			sink.attach_source(src);
		}

		Ok(Runtime { nodes })

		/* let mut sources: HashMap<String, Box<dyn Source>> = HashMap::new();
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
		}) */
	}
}
