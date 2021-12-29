use smartstring::alias::String as SmartString;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::net;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "debug")]
use std::time;

use serde::{de, Deserialize as DeserializeTrait, Deserializer};
use serde_derive::Deserialize;

use glob;

#[cfg(feature = "debug")]
use super::debug;
#[cfg(feature = "detrend")]
use super::detrend;
#[cfg(feature = "fft")]
use super::fft;
use super::filter;
#[cfg(feature = "influxdb")]
use super::influxdb;
#[cfg(feature = "pubsub")]
use super::pubsub;
#[cfg(feature = "relay")]
use super::relay;
use super::router;
#[cfg(feature = "sbx")]
use super::sbx::SBXSource;
#[cfg(feature = "smbus")]
use super::smbus;
#[cfg(feature = "stream-filearchive")]
use super::stream as runtime_stream;
#[cfg(feature = "summary")]
use super::summary;
use super::traits;

use crate::metric;
use crate::script;
#[cfg(feature = "sbx")]
use crate::snurl;
#[cfg(feature = "debug")]
use crate::stream;

#[derive(Debug)]
pub enum BuildError {
	UndefinedSink {
		which: String,
	},
	NotASink {
		which: String,
	},
	UndefinedSource {
		which: String,
	},
	NotASource {
		which: String,
	},
	FeatureNotAvailable {
		which: String,
		feature_name: &'static str,
	},
	Other(Box<dyn Error>),
}

impl fmt::Display for BuildError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::UndefinedSink { which } => {
				write!(f, "undefined sink {:?}", which)
			}
			Self::NotASink { which } => {
				write!(f, "{:?} is not a sink", which)
			}
			Self::UndefinedSource { which } => {
				write!(f, "undefined source {:?}", which)
			}
			Self::NotASource { which } => {
				write!(f, "{:?} is not a source", which)
			}
			Self::FeatureNotAvailable {
				which,
				feature_name,
			} => {
				write!(
					f,
					"{:?} is only available if built with the {} feature",
					which, feature_name
				)
			}
			Self::Other(e) => write!(f, "{:?}", e),
		}
	}
}

impl Error for BuildError {}

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

fn f64_one() -> f64 {
	1.0
}

fn f64_zero() -> f64 {
	0.0
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
	where
		D: Deserializer<'de>,
	{
		let s = String::deserialize(deserializer)?;
		let pattern = s.parse::<glob::Pattern>().map_err(de::Error::custom)?;
		Ok(PatternWrap(pattern))
	}
}

#[derive(Debug, Clone)]
#[cfg(feature = "regex")]
pub struct RegexWrap(pub regex::Regex);

#[cfg(feature = "regex")]
impl Deref for RegexWrap {
	type Target = regex::Regex;

	fn deref(&self) -> &regex::Regex {
		&self.0
	}
}

#[cfg(feature = "regex")]
impl DerefMut for RegexWrap {
	fn deref_mut(&mut self) -> &mut regex::Regex {
		&mut self.0
	}
}

#[cfg(feature = "regex")]
impl<'de> DeserializeTrait<'de> for RegexWrap {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let s = String::deserialize(deserializer)?;
		let pattern = s.parse::<regex::Regex>().map_err(de::Error::custom)?;
		Ok(RegexWrap(pattern))
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
	where
		D: Deserializer<'de>,
	{
		let s = String::deserialize(deserializer)?;
		let s = s
			.parse::<Box<dyn script::Evaluate>>()
			.map_err(de::Error::custom)?;
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
	where
		D: Deserializer<'de>,
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
#[serde(tag = "mode")]
pub enum StreamBufferConfig {
	InMemory { slice: i64 },
}

impl StreamBufferConfig {
	#[cfg(feature = "debug")]
	fn build(&self) -> Box<dyn stream::StreamBuffer + Send + Sync + 'static> {
		match self {
			Self::InMemory { slice } => Box::new(stream::InMemoryBuffer::new(
				chrono::Duration::milliseconds(*slice),
			)),
		}
	}
}

#[cfg(feature = "influxdb")]
#[derive(Debug, Clone, Deserialize)]
pub struct InfluxDBPredicate {
	match_measurement: Option<PatternWrap>,
	#[serde(default = "bool_false")]
	invert: bool,
}

#[cfg(feature = "influxdb")]
impl InfluxDBPredicate {
	fn build(&self) -> crate::influxdb::Select {
		crate::influxdb::Select {
			invert: self.invert,
			match_measurement: self
				.match_measurement
				.as_ref()
				.and_then(|x| Some(x.0.clone())),
		}
	}
}

#[cfg(feature = "influxdb")]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum InfluxDBMapping {
	Transpose {
		predicate: Option<InfluxDBPredicate>,
		tag: String,
		field: String,
	},
	#[cfg(feature = "regex")]
	TagToField {
		predicate: Option<InfluxDBPredicate>,
		expr: RegexWrap,
		new_tag_value: String,
		field_name: String,
	},
}

#[cfg(feature = "influxdb")]
impl InfluxDBMapping {
	fn build(&self) -> Result<Box<dyn crate::influxdb::Filter>, BuildError> {
		match self {
			Self::Transpose {
				predicate,
				tag,
				field,
			} => Ok(Box::new(crate::influxdb::Transpose {
				predicate: predicate
					.clone()
					.and_then(|cfg| Some(cfg.build()))
					.unwrap_or_else(|| crate::influxdb::Select::default()),
				tag: tag.clone().into(),
				field: field.clone().into(),
			})),
			#[cfg(feature = "regex")]
			Self::TagToField {
				predicate,
				expr,
				new_tag_value,
				field_name,
			} => Ok(Box::new(crate::influxdb::TagToField {
				predicate: predicate
					.clone()
					.and_then(|cfg| Some(cfg.build()))
					.unwrap_or_else(|| crate::influxdb::Select::default()),
				expr: expr.0.clone(),
				new_tag_value: new_tag_value.clone().into(),
				field_name: field_name.clone().into(),
			})),
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
pub enum BME280Instance {
	Primary,
	Secondary,
}

impl BME280Instance {
	#[cfg(feature = "smbus")]
	fn addr(&self) -> u8 {
		match self {
			Self::Primary => 0x76,
			Self::Secondary => 0x77,
		}
	}
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum DetrendMode {
	Constant,
	Linear,
}

#[cfg(feature = "detrend")]
impl From<DetrendMode> for detrend::Mode {
	fn from(other: DetrendMode) -> Self {
		match other {
			DetrendMode::Constant => Self::Constant,
			DetrendMode::Linear => Self::Linear,
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "class")]
pub enum Node {
	SBX {
		path_prefix: String,
		transport: SNURLConfig,
	},
	Random {
		device_type: String,
		instance: String,
		interval: f64,
		components: HashMap<String, RandomComponent>,
	},
	Listen {
		listen_address: String,
	},
	Connect {
		peer_address: String,
	},
	DebugStdout,
	Route {
		filters: Vec<Filter>,
	},
	#[cfg(feature = "influxdb")]
	InfluxDB {
		api_url: String,
		auth: crate::influxdb::Auth,
		database: String,
		retention_policy: Option<String>,
		precision: crate::influxdb::Precision,
		mapping: Vec<InfluxDBMapping>,
	},
	PubSub {
		api_url: String,
		node_template: String,
		override_host: Option<String>,
	},
	Sine {
		nsamples: u16,
		sample_period: u16,
		instance: String,
		device_type: String,
		#[serde(default = "f64_one")]
		amplitude: f64,
		#[serde(default = "f64_zero")]
		offset: f64,
		scale: f64,
		period: f64,
		phase: f64,
		buffer: StreamBufferConfig,
	},
	FFT {
		size: usize,
	},
	Summary {
		size: usize,
	},
	BME280 {
		device: PathBuf,
		path_prefix: String,
		instance: BME280Instance,
		interval: u32,
		reconfigure_each: Option<u32>,
	},
	#[cfg(feature = "stream-filearchive")]
	SimpleFileArchive {
		path: PathBuf,
	},
	Detrend {
		mode: DetrendMode,
	},
}

impl Node {
	pub fn build(&self) -> Result<traits::Node, BuildError> {
		match self {
			Self::SBX {
				path_prefix,
				transport,
			} => {
				#[cfg(feature = "sbx")]
				{
					let raw_sock = match net::UdpSocket::bind(net::SocketAddr::new(
						transport.local_address,
						transport.local_port,
					)) {
						Err(e) => return Err(BuildError::Other(Box::new(e))),
						Ok(s) => s,
					};

					raw_sock
						.set_nonblocking(true)
						.expect("setting the udp socket to be non-blocking");
					let sock = snurl::Socket::new(
						tokio::net::UdpSocket::from_std(raw_sock)
							.expect("conversion to tokio socket"),
						net::SocketAddr::new(transport.remote_address, transport.remote_port),
					);
					let ep = snurl::Endpoint::new(sock);
					Ok(traits::Node::from_source(SBXSource::new(
						ep,
						path_prefix.clone(),
					)))
				}
				#[cfg(not(feature = "sbx"))]
				{
					let _ = (path_prefix, transport);
					Err(BuildError::FeatureNotAvailable {
						which: "SBXSource node".into(),
						feature_name: "sbx",
					})
				}
			}
			Self::Random {
				device_type,
				instance,
				interval,
				components,
			} => {
				#[cfg(feature = "debug")]
				{
					let mut components_out: metric::OrderedVec<
						SmartString,
						debug::RandomComponent,
					> = metric::OrderedVec::new();
					for (k, v) in components.iter() {
						components_out.insert(
							k.into(),
							debug::RandomComponent {
								unit: v.unit.0.clone(),
								min: v.min,
								max: v.max,
							},
						);
					}
					Ok(traits::Node::from_source(debug::RandomSource::new(
						time::Duration::from_secs_f64(*interval),
						instance.into(),
						device_type.into(),
						components_out,
					)))
				}
				#[cfg(not(feature = "debug"))]
				{
					let _ = (device_type, instance, interval, components);
					Err(BuildError::FeatureNotAvailable {
						which: "Random node".into(),
						feature_name: "debug",
					})
				}
			}
			Self::Listen { listen_address } => {
				#[cfg(feature = "relay")]
				{
					let raw_sock = match net::TcpListener::bind(&listen_address[..]) {
						Err(e) => return Err(BuildError::Other(Box::new(e))),
						Ok(s) => s,
					};
					raw_sock
						.set_nonblocking(true)
						.expect("setting the tcp socket to be non-blocking");
					Ok(traits::Node::from_source(relay::RelaySource::new(
						tokio::net::TcpListener::from_std(raw_sock)
							.expect("conversion to tokio socket"),
					)))
				}
				#[cfg(not(feature = "relay"))]
				{
					let _ = listen_address;
					Err(BuildError::FeatureNotAvailable {
						which: "Listen node".into(),
						feature_name: "relay",
					})
				}
			}
			Self::Connect { peer_address } => {
				#[cfg(feature = "relay")]
				{
					Ok(traits::Node::from_sink(relay::RelaySink::new(
						peer_address.clone(),
					)))
				}
				#[cfg(not(feature = "relay"))]
				{
					let _ = peer_address;
					Err(BuildError::FeatureNotAvailable {
						which: "Connect node".into(),
						feature_name: "relay",
					})
				}
			}
			Self::DebugStdout => {
				#[cfg(feature = "debug")]
				{
					Ok(traits::Node::from_sink(debug::DebugStdoutSink::new()))
				}
				#[cfg(not(feature = "debug"))]
				{
					Err(BuildError::FeatureNotAvailable {
						which: "DebugStdout node".into(),
						feature_name: "debug",
					})
				}
			}
			Self::Route { filters } => {
				let mut built_filters = Vec::new();
				for filter in filters.iter() {
					built_filters.push(filter.build()?);
				}
				Ok(traits::Node::from(router::Router::new(built_filters)))
			}
			#[cfg(feature = "influxdb")]
			Self::InfluxDB {
				api_url,
				auth,
				database,
				retention_policy,
				precision,
				mapping,
			} => {
				let mut built_filters = Vec::new();
				for filter in mapping.iter() {
					built_filters.push(filter.build()?);
				}
				Ok(traits::Node::from_sink(influxdb::InfluxDBSink::new(
					api_url.clone(),
					auth.clone(),
					database.clone(),
					retention_policy.clone(),
					*precision,
					built_filters,
				)))
			}
			Self::PubSub {
				api_url,
				node_template,
				override_host,
			} => {
				#[cfg(feature = "pubsub")]
				{
					Ok(traits::Node::from_sink(pubsub::PubSubSink::new(
						api_url.clone(),
						node_template.clone(),
						override_host.clone(),
					)))
				}
				#[cfg(not(feature = "pubsub"))]
				{
					let _ = (api_url, node_template, override_host);
					Err(BuildError::FeatureNotAvailable {
						which: "PubSub node".into(),
						feature_name: "pubsub",
					})
				}
			}
			Self::Sine {
				nsamples,
				sample_period,
				instance,
				device_type,
				scale,
				amplitude,
				offset,
				period,
				phase,
				buffer,
			} => {
				#[cfg(feature = "debug")]
				{
					Ok(traits::Node::from_source(debug::SineSource::new(
						*nsamples,
						time::Duration::from_millis(*sample_period as u64),
						metric::DevicePath {
							instance: instance.into(),
							device_type: device_type.into(),
						},
						metric::Value {
							unit: metric::Unit::Arbitrary,
							magnitude: *scale,
						},
						debug::SineConfig {
							amplitude: *amplitude,
							offset: *offset,
							phase: *phase,
							period: *period,
						},
						buffer.build(),
					)))
				}
				#[cfg(not(feature = "debug"))]
				{
					let _ = (
						nsamples,
						sample_period,
						instance,
						device_type,
						scale,
						amplitude,
						offset,
						period,
						phase,
						buffer,
					);
					Err(BuildError::FeatureNotAvailable {
						which: "Sine node".into(),
						feature_name: "debug",
					})
				}
			}
			Self::FFT { size } => {
				#[cfg(feature = "fft")]
				{
					Ok(traits::Node::from(fft::Fft::new(*size)))
				}
				#[cfg(not(feature = "fft"))]
				{
					let _ = size;
					Err(BuildError::FeatureNotAvailable {
						which: "FFT node".into(),
						feature_name: "fft",
					})
				}
			}
			Self::Summary { size } => {
				#[cfg(feature = "summary")]
				{
					Ok(traits::Node::from(summary::Summary::new(*size)))
				}
				#[cfg(not(feature = "summary"))]
				{
					let _ = size;
					Err(BuildError::FeatureNotAvailable {
						which: "Summary node".into(),
						feature_name: "summary",
					})
				}
			}
			Self::BME280 {
				device,
				path_prefix,
				instance,
				interval,
				reconfigure_each,
			} => {
				#[cfg(feature = "smbus")]
				{
					let node = match smbus::BME280::new(
						device,
						instance.addr(),
						path_prefix.into(),
						std::time::Duration::from_millis(*interval as u64),
						reconfigure_each.unwrap_or(1024) as usize,
					) {
						Ok(v) => v,
						Err(e) => return Err(BuildError::Other(Box::new(e))),
					};
					Ok(traits::Node::from_source(node))
				}
				#[cfg(not(feature = "smbus"))]
				{
					let _ = (device, path_prefix, instance, interval, reconfigure_each);
					Err(BuildError::FeatureNotAvailable {
						which: "BME280 node".into(),
						feature_name: "smbus",
					})
				}
			}
			#[cfg(feature = "stream-filearchive")]
			Self::SimpleFileArchive { path } => {
				let dir = match openat::Dir::open(path) {
					Ok(v) => v,
					Err(e) => return Err(BuildError::Other(Box::new(e))),
				};
				let archive = Box::new(stream::SimpleFileArchive::new(dir, 0o640));
				Ok(traits::Node::from_sink(runtime_stream::Archiver::new(
					archive,
				)))
			}
			Self::Detrend { mode } => {
				#[cfg(feature = "detrend")]
				{
					Ok(traits::Node::from(detrend::Detrend::new(
						mode.clone().into(),
					)))
				}
				#[cfg(not(feature = "detrend"))]
				{
					let _ = mode;
					Err(BuildError::FeatureNotAvailable {
						which: "Detrend node".into(),
						feature_name: "detrend",
					})
				}
			}
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
		Ok(filter::SelectByPath {
			invert: self.invert,
			match_device_type: self.match_device_type.clone().and_then(|p| Some(p.0)),
			match_instance: self.match_instance.clone().and_then(|p| Some(p.0)),
		})
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Filter {
	SelectByPath {
		#[serde(default = "bool_false")]
		invert: bool,
		match_device_type: Option<PatternWrap>,
		match_instance: Option<PatternWrap>,
	},
	Calc {
		predicate: Option<FilterPredicate>,
		script: ScriptWrap,
		new_component: String,
		new_unit: UnitWrap,
	},
	DropComponent {
		predicate: Option<FilterPredicate>,
		component_name: String,
	},
	KeepComponent {
		predicate: Option<FilterPredicate>,
		component_name: String,
	},
	MapInstance {
		predicate: Option<FilterPredicate>,
		mapping: HashMap<SmartString, SmartString>,
	},
	MapDeviceType {
		predicate: Option<FilterPredicate>,
		mapping: HashMap<SmartString, SmartString>,
	},
	Map {
		predicate: Option<FilterPredicate>,
		script: ScriptWrap,
		new_unit: UnitWrap,
	},
}

impl Filter {
	pub fn build(&self) -> Result<Box<dyn filter::Filter>, BuildError> {
		match self {
			Self::SelectByPath {
				invert,
				match_device_type,
				match_instance,
			} => Ok(Box::new(filter::SelectByPath {
				invert: *invert,
				match_device_type: match_device_type.clone().and_then(|p| Some(p.0)),
				match_instance: match_instance.clone().and_then(|p| Some(p.0)),
			})),
			Self::Calc {
				predicate,
				script,
				new_component,
				new_unit,
			} => Ok(Box::new(filter::Calc {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				script: script.0.clone(),
				new_component: new_component.into(),
				new_unit: new_unit.0.clone(),
			})),
			Self::DropComponent {
				predicate,
				component_name,
			} => Ok(Box::new(filter::DropComponent {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				component_name: component_name.into(),
			})),
			Self::KeepComponent {
				predicate,
				component_name,
			} => Ok(Box::new(filter::KeepComponent {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				component_name: component_name.into(),
			})),
			Self::MapInstance { predicate, mapping } => Ok(Box::new(filter::MapInstance {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				mapping: mapping.clone(),
			})),
			Self::MapDeviceType { predicate, mapping } => Ok(Box::new(filter::MapDeviceType {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				mapping: mapping.clone(),
			})),
			Self::Map {
				predicate,
				script,
				new_unit,
			} => Ok(Box::new(filter::Map {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				script: script.0.clone(),
				new_unit: new_unit.0.clone(),
			})),
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
pub struct Link {
	pub source: String,
	pub sink: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
	pub node: HashMap<String, Node>,
	pub link: Vec<Link>,
}
