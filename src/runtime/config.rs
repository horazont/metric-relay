use std::collections::HashMap;
use smartstring::alias::{String as SmartString};
use std::sync::Arc;
use std::fmt;
use std::error::Error;
use std::net;
use std::ops::{Deref, DerefMut};
use core::time;

use serde::{de, Deserializer, Deserialize as DeserializeTrait};
use serde_derive::{Deserialize};

use glob;

use super::traits;
use super::sbx::SBXSource;
use super::debug;
use super::filter;
use super::relay;

use crate::metric;
use crate::script;
use crate::snurl;

#[derive(Debug)]
pub enum BuildError {
	UndefinedSink{which: String, at: String},
	UndefinedSource{which: String, at: String},
	Other(Box<dyn Error>),
}

impl fmt::Display for BuildError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::UndefinedSink{which, at} => {
				write!(f, "undefined sink {:?} {}", which, at)
			},
			Self::UndefinedSource{which, at} => {
				write!(f, "undefined sink {:?} {}", which, at)
			},
			Self::Other(e) => write!(f, "{:?}", e),
		}
	}
}

impl Error for BuildError {
}

fn default_local_address() -> net::IpAddr {
	"0.0.0.0".parse::<net::IpAddr>().unwrap()
}

fn default_remote_address() -> net::IpAddr {
	"255.255.255.255".parse::<net::IpAddr>().unwrap()
}

fn default_filters() -> Vec<Filter> {
	Vec::new()
}

fn bool_false() -> bool {
	false
}

#[derive(Debug, Clone)]
pub struct PatternWrap(pub glob::Pattern);

impl Deref for PatternWrap {
	type Target = glob::Pattern;

	fn deref(&self) -> &glob::Pattern {
		&self.0
	}
}

impl DerefMut for PatternWrap {
	fn deref_mut(&mut self) -> &mut glob::Pattern {
		&mut self.0
	}
}

impl<'de> DeserializeTrait<'de> for PatternWrap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        let pattern = s.parse::<glob::Pattern>().map_err(de::Error::custom)?;
        Ok(PatternWrap(pattern))
    }
}

#[derive(Debug, Clone)]
pub struct ScriptWrap(pub Arc<Box<dyn script::Evaluate>>);

impl Deref for ScriptWrap {
	type Target = dyn script::Evaluate;

	fn deref(&self) -> &(dyn script::Evaluate + 'static) {
		self.0.deref().deref()
	}
}

impl<'de> DeserializeTrait<'de> for ScriptWrap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        let s = s.parse::<Box<dyn script::Evaluate>>().map_err(de::Error::custom)?;
        Ok(ScriptWrap(Arc::new(s)))
    }
}

#[derive(Debug, Clone)]
pub struct UnitWrap(pub metric::Unit);

impl Deref for UnitWrap {
	type Target = metric::Unit;

	fn deref(&self) -> &metric::Unit {
		&self.0
	}
}

impl DerefMut for UnitWrap {
	fn deref_mut(&mut self) -> &mut metric::Unit {
		&mut self.0
	}
}

impl<'de> DeserializeTrait<'de> for UnitWrap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let s = String::deserialize(deserializer)?;
        let unit = s.parse::<metric::Unit>().map_err(de::Error::custom)?;
        Ok(UnitWrap(unit))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SNURLConfig {
	#[serde(default = "default_local_address")]
	local_address: net::IpAddr,
	local_port: u16,
	#[serde(default = "default_remote_address")]
	remote_address: net::IpAddr,
	remote_port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RandomComponent {
	unit: UnitWrap,
	min: f64,
	max: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "class")]
pub enum Source {
	SBX{
		path_prefix: String,
		transport: SNURLConfig,
	},
	Random{
		device_type: String,
		instance: String,
		interval: f64,
		components: HashMap<String, RandomComponent>,
	},
	Relay{
		listen_address: String,
	},
}

impl Source {
	pub fn build(&self) -> Result<Box<dyn traits::Source>, BuildError> {
		match self {
			Self::SBX{path_prefix, transport} => {
				let raw_sock = match net::UdpSocket::bind(net::SocketAddr::new(transport.local_address, transport.local_port)) {
					Err(e) => {
						return Err(BuildError::Other(Box::new(e)))
					},
					Ok(s) => s,
				};

				raw_sock.set_nonblocking(true).expect("setting the udp socket to be non-blocking");
				let sock = snurl::Socket::new(
					tokio::net::UdpSocket::from_std(raw_sock).expect("conversion to tokio socket"),
					net::SocketAddr::new(transport.remote_address, transport.remote_port),
				);
				let ep = snurl::Endpoint::new(sock);
				Ok(Box::new(SBXSource::new(ep, path_prefix.clone())))
			},
			Self::Random{device_type, instance, interval, components} => {
				let mut components_out: metric::OrderedVec<SmartString, debug::RandomComponent> = metric::OrderedVec::new();
				for (k, v) in components.iter() {
					components_out.insert(k.into(), debug::RandomComponent{
						unit: v.unit.0.clone(),
						min: v.min,
						max: v.max,
					});
				};
				Ok(Box::new(debug::RandomSource::new(
					time::Duration::from_secs_f64(*interval),
					instance.into(),
					device_type.into(),
					components_out,
				)))
			},
			Self::Relay{listen_address} => {
				let raw_sock = match net::TcpListener::bind(&listen_address[..]) {
					Err(e) => return Err(BuildError::Other(Box::new(e))),
					Ok(s) => s,
				};
				raw_sock.set_nonblocking(true).expect("setting the tcp socket to be non-blocking");

				Ok(Box::new(relay::RelaySource::new(
					tokio::net::TcpListener::from_std(raw_sock).expect("conversion to tokio socket"),
				)))
			},
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "class")]
pub enum Sink {
	DebugStdout,
	Relay{
		peer_address: String,
	},
}
impl Sink {
	pub fn build(&self) -> Result<Box<dyn traits::Sink>, BuildError> {
		match self {
			Self::DebugStdout => {
				Ok(Box::new(debug::DebugStdoutSink::new()))
			},
			Self::Relay{peer_address} => {
				Ok(Box::new(relay::RelaySink::new(peer_address.clone())))
			},
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub struct FilterPredicate {
	#[serde(default = "bool_false")]
	invert: bool,
	match_device_type: Option<PatternWrap>,
	match_instance: Option<PatternWrap>,
}

impl FilterPredicate {
	pub fn build(&self) -> Result<filter::SelectByPath, BuildError> {
		Ok(filter::SelectByPath{
			invert: self.invert,
			match_device_type: self.match_device_type.clone().and_then(|p| { Some(p.0) }),
			match_instance: self.match_instance.clone().and_then(|p| { Some(p.0) }),
		})
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Filter {
	SelectByPath{
		#[serde(default = "bool_false")]
		invert: bool,
		match_device_type: Option<PatternWrap>,
		match_instance: Option<PatternWrap>,
	},
	Calc{
		predicate: Option<FilterPredicate>,
		script: ScriptWrap,
		new_component: String,
		new_unit: UnitWrap,
	},
	DropComponent{
		predicate: Option<FilterPredicate>,
		component_name: String,
	},
	MapInstance{
		predicate: Option<FilterPredicate>,
		mapping: HashMap<SmartString, SmartString>,
	},
}

impl Filter {
	pub fn build(&self) -> Result<Box<dyn filter::Filter>, BuildError> {
		match self {
			Self::SelectByPath{invert, match_device_type, match_instance} => {
				Ok(Box::new(filter::SelectByPath{
					invert: *invert,
					match_device_type: match_device_type.clone().and_then(|p| { Some(p.0) }),
					match_instance: match_instance.clone().and_then(|p| { Some(p.0) }),
				}))
			},
			Self::Calc{predicate, script, new_component, new_unit} => {
				Ok(Box::new(filter::Calc{
					predicate: match predicate {
						Some(p) => p.build()?,
						None => filter::SelectByPath::default(),
					},
					script: script.0.clone(),
					new_component: new_component.into(),
					new_unit: new_unit.0.clone(),
				}))
			},
			Self::DropComponent{predicate, component_name} => {
				Ok(Box::new(filter::DropComponent{
					predicate: match predicate {
						Some(p) => p.build()?,
						None => filter::SelectByPath::default(),
					},
					component_name: component_name.into(),
				}))
			},
			Self::MapInstance{predicate, mapping} => {
				Ok(Box::new(filter::MapInstance{
					predicate: match predicate {
						Some(p) => p.build()?,
						None => filter::SelectByPath::default(),
					},
					mapping: mapping.clone(),
				}))
			},
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
pub struct Route {
	pub source: String,
	pub sink: String,
	#[serde(default = "default_filters")]
	pub filters: Vec<Filter>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
	pub sources: HashMap<String, Source>,
	pub sinks: HashMap<String, Sink>,
	pub routes: Vec<Route>,
}
