use std::collections::HashMap;
use std::error::Error;
use std::fmt;
#[cfg(feature = "sbm")]
use std::io;
use std::net;
#[cfg(feature = "sbm")]
use std::net::IpAddr;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "debug")]
use std::time;

use smartstring::alias::String as SmartString;

use serde::{de, Deserialize as DeserializeTrait, Deserializer};
use serde_derive::Deserialize;

use log::Level;

use glob;

#[cfg(feature = "csv")]
use super::csvinject;
#[cfg(feature = "debug")]
use super::debug;
#[cfg(feature = "detrend")]
use super::detrend;
#[cfg(feature = "fft")]
use super::fft;
use super::filter;
use super::hwmon;
#[cfg(feature = "influxdb")]
use super::influxdb;
#[cfg(feature = "pubsub")]
use super::pubsub;
#[cfg(feature = "relay")]
use super::relay;
use super::router;
use super::samplify;
#[cfg(feature = "sbm")]
use super::sbm;
#[cfg(feature = "sbx")]
use super::sbx;
#[cfg(feature = "smbus")]
use super::smbus;
#[cfg(feature = "stream-filearchive")]
use super::stream as runtime_stream;
use super::streamify;
#[cfg(feature = "summary")]
use super::summary;
use super::traits;

use crate::metric;
use crate::script;
#[cfg(feature = "sbm")]
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

#[cfg_attr(not(feature = "sbx"), allow(dead_code))]
#[derive(Debug, Clone, Deserialize)]
pub struct SNURLConfig {
	#[serde(default = "default_local_address")]
	local_address: net::IpAddr,
	local_port: u16,
	#[serde(default = "default_remote_address")]
	remote_address: net::IpAddr,
	remote_port: u16,
	multicast_group: Option<net::IpAddr>,
	#[serde(default = "bool_false")]
	passive: bool,
}

#[cfg(feature = "serial")]
#[derive(Debug, Clone, Deserialize)]
pub struct SerialConfig {
	port: String,
	baudrate: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum SBXTransportConfig {
	SNURL(SNURLConfig),
	#[cfg(feature = "serial")]
	Serial(SerialConfig),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RandomComponent {
	#[cfg_attr(not(feature = "debug"), allow(dead_code))]
	unit: UnitWrap,
	#[cfg_attr(not(feature = "debug"), allow(dead_code))]
	min: f64,
	#[cfg_attr(not(feature = "debug"), allow(dead_code))]
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
pub struct StreamifyDescription {
	device_type: String,
	instance: String,
	component: String,
	period_ms: u64,
	slice_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HwmonSensor {
	name: String,
	sensor: u32,
	#[serde(rename = "type")]
	type_: hwmon::Type,
	component: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(not(feature = "csv"), allow(dead_code))]
pub struct CsvComponentMapping {
	column: String,
	component: String,
	unit: UnitWrap,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "class")]
pub enum Node {
	SBX {
		path_prefix: String,
		#[serde(default = "bool_false")]
		rewrite_bme68x: bool,
		transport: SBXTransportConfig,
	},
	Mininode {
		path_prefix: String,
		#[serde(default = "bool_false")]
		rewrite_bme68x: bool,
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
	Streamify {
		stream: Vec<StreamifyDescription>,
	},
	Hwmon {
		device_type: String,
		instance: String,
		interval: u32,
		sensors: Vec<HwmonSensor>,
	},
	FromCsv {
		filename: String,
		device_type_column: String,
		instance_column: String,
		timestamp_column: String,
		components: Vec<CsvComponentMapping>,
		batch_size: usize,
		start_time: chrono::DateTime<chrono::Utc>,
		end_time: chrono::DateTime<chrono::Utc>,
		sleep_ms: u32,
	},
	Samplify {
		component: String,
	},
}

impl Node {
	pub fn build(&self) -> Result<traits::Node, BuildError> {
		match self {
			Self::SBX {
				path_prefix,
				rewrite_bme68x,
				transport,
			} => {
				#[cfg(feature = "sbx")]
				{
					let source = match transport {
						SBXTransportConfig::SNURL(transport) => {
							let transport = transport.clone();
							sbx::SBXSource::new(
								Box::new(move || -> io::Result<snurl::Endpoint> {
									let raw_sock = net::UdpSocket::bind(net::SocketAddr::new(
										transport.local_address,
										transport.local_port,
									))?;

									raw_sock.set_nonblocking(true)?;
									if let Some(multicast_group) =
										transport.multicast_group.as_ref()
									{
										match (multicast_group, transport.local_address) {
										(IpAddr::V4(mc_group), IpAddr::V4(ifaddr)) => {
											raw_sock.join_multicast_v4(mc_group, &ifaddr)?
										},
										(IpAddr::V6(_), IpAddr::V6(_)) => {
											panic!("multicast operation not supported on ipv6")
										},
										_ => panic!("multicast group address family differs from local address family")
									}
									}
									let sock = snurl::Socket::new(
										tokio::net::UdpSocket::from_std(raw_sock)
											.expect("conversion to tokio socket"),
										net::SocketAddr::new(
											transport.remote_address,
											transport.remote_port,
										),
										transport.passive,
									);
									Ok(snurl::Endpoint::new(sock))
								}),
								path_prefix.clone(),
								*rewrite_bme68x,
							)
							.map_err(|e| BuildError::Other(Box::new(e)))?
						}
						#[cfg(feature = "serial")]
						SBXTransportConfig::Serial(transport) => sbx::SBXSource::with_serial(
							tokio_serial::SerialStream::open(&tokio_serial::new(
								&transport.port,
								transport.baudrate,
							))
							.expect("open serial port"),
							path_prefix.clone(),
							*rewrite_bme68x,
						),
					};
					Ok(traits::Node::from_source(source))
				}
				#[cfg(not(feature = "sbx"))]
				{
					let _ = (path_prefix, rewrite_bme68x, transport);
					Err(BuildError::FeatureNotAvailable {
						which: "SBXSource node".into(),
						feature_name: "sbx",
					})
				}
			}
			Self::Mininode {
				path_prefix,
				rewrite_bme68x,
				transport,
			} => {
				#[cfg(feature = "sbm")]
				{
					let transport = transport.clone();
					let source = sbm::MininodeSource::new(
						Box::new(move || -> io::Result<snurl::Endpoint> {
							let raw_sock = net::UdpSocket::bind(net::SocketAddr::new(
								transport.local_address,
								transport.local_port,
							))?;

							raw_sock.set_nonblocking(true)?;
							if let Some(multicast_group) = transport.multicast_group.as_ref() {
								match (multicast_group, transport.local_address) {
								(IpAddr::V4(mc_group), IpAddr::V4(ifaddr)) => {
									raw_sock.join_multicast_v4(mc_group, &ifaddr)?
								},
								(IpAddr::V6(_), IpAddr::V6(_)) => {
									panic!("multicast operation not supported on ipv6")
								},
								_ => panic!("multicast group address family differs from local address family")
							}
							}
							let sock = snurl::Socket::new(
								tokio::net::UdpSocket::from_std(raw_sock)
									.expect("conversion to tokio socket"),
								net::SocketAddr::new(
									transport.remote_address,
									transport.remote_port,
								),
								transport.passive,
							);
							Ok(snurl::Endpoint::new(sock))
						}),
						path_prefix.clone(),
						*rewrite_bme68x,
					)
					.map_err(|e| BuildError::Other(Box::new(e)))?;
					Ok(traits::Node::from_source(source))
				}
				#[cfg(not(feature = "sbm"))]
				{
					let _ = (path_prefix, rewrite_bme68x, transport);
					Err(BuildError::FeatureNotAvailable {
						which: "Mininode node".into(),
						feature_name: "sbm",
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
			Self::Streamify { stream: streams } => {
				let mut descriptors = HashMap::new();
				for stream in streams.iter() {
					let descriptor = streamify::Descriptor::new(
						stream.component.clone().into(),
						std::time::Duration::from_millis(stream.period_ms),
						chrono::Duration::milliseconds(stream.slice_ms),
					);
					descriptors.insert(
						metric::DevicePath {
							device_type: stream.device_type.clone().into(),
							instance: stream.instance.clone().into(),
						},
						descriptor,
					);
				}
				Ok(traits::Node::from(streamify::Streamify::new(descriptors)))
			}
			Self::Hwmon {
				interval,
				device_type,
				instance,
				sensors: sensor_defs,
			} => {
				let mut sensors = Vec::new();
				for def in sensor_defs.iter() {
					match hwmon::Sensor::new(
						&def.name,
						def.sensor,
						def.type_,
						def.component.clone().into(),
					) {
						Ok(v) => sensors.push(v),
						Err(e) => return Err(BuildError::Other(Box::new(e))),
					}
				}
				Ok(traits::Node::from_source(hwmon::Hwmon::new(
					hwmon::Scrape::new(
						std::time::Duration::from_millis(*interval as u64),
						metric::DevicePath {
							device_type: device_type.clone().into(),
							instance: instance.clone().into(),
						},
						sensors,
					),
				)))
			}
			Self::FromCsv {
				filename,
				device_type_column,
				instance_column,
				timestamp_column,
				components,
				start_time,
				end_time,
				batch_size,
				sleep_ms,
			} => {
				#[cfg(feature = "csv")]
				{
					let file = match std::fs::File::open(filename) {
						Ok(v) => v,
						Err(e) => return Err(BuildError::Other(Box::new(e))),
					};
					let mut component_mapping = Vec::with_capacity(components.len());
					for decl in components.iter() {
						component_mapping.push((
							decl.column.as_str(),
							decl.component.clone().into(),
							decl.unit.0.clone(),
						));
					}
					let node = match csvinject::Injector::new(
						Box::new(file),
						device_type_column,
						instance_column,
						timestamp_column,
						component_mapping,
						*start_time,
						*end_time,
						chrono::Duration::seconds(0),
						*batch_size,
						std::time::Duration::from_millis(*sleep_ms as u64),
					) {
						Ok(v) => v,
						Err(e) => return Err(BuildError::Other(Box::new(e))),
					};
					Ok(traits::Node::from_source(node))
				}
				#[cfg(not(feature = "csv"))]
				{
					let _ = (
						filename,
						device_type_column,
						instance_column,
						timestamp_column,
						components,
						start_time,
						end_time,
						batch_size,
						sleep_ms,
					);
					Err(BuildError::FeatureNotAvailable {
						which: "FromCsv node".into(),
						feature_name: "csv",
					})
				}
			}
			Self::Samplify { component } => Ok(traits::Node::from(samplify::Samplify::new(
				component.into(),
			))),
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
	MapInstanceValue {
		predicate: Option<FilterPredicate>,
		unit: UnitWrap,
		component_name: String,
		mapping: HashMap<SmartString, f64>,
	},
	KeepIfPlausible {
		predicate: Option<FilterPredicate>,
		#[serde(default = "bool_false")]
		log_loudly: bool,
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
			Self::MapInstanceValue {
				predicate,
				component_name,
				unit,
				mapping,
			} => Ok(Box::new(filter::MapInstanceValue {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				unit: unit.0.clone(),
				component: component_name.into(),
				mapping: mapping.clone(),
			})),
			Self::KeepIfPlausible {
				predicate,
				log_loudly,
			} => Ok(Box::new(filter::KeepIfPlausible {
				predicate: match predicate {
					Some(p) => p.build()?,
					None => filter::SelectByPath::default(),
				},
				loglevel: if *log_loudly {
					Level::Warn
				} else {
					Level::Debug
				},
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
