use std::fmt;
use std::io;

use reqwest;
use base64;
use bytes::{BytesMut, BufMut};
use chrono::{DateTime, Utc};

use serde_derive::{Serialize, Deserialize};

use crate::metric;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Auth {
	None,
	HTTP{username: String, password: String},
	Query{username: String, password: String},
}

impl Auth {
	pub fn apply(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		match self {
			Self::None => req,
			Self::HTTP{username, password} => req.header("Authorization", base64::encode(format!(
				"{}:{}", username, password,
			))),
			Self::Query{username, password} => req.query(&[("u", username), ("p", password)]),
		}
	}
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Precision {
	Nanoseconds,
	Microseconds,
	Milliseconds,
	Seconds,
}

impl Precision {
	pub fn value(&self) -> &'static str {
		match self {
			Self::Nanoseconds => "ns",
			Self::Microseconds => "u",
			Self::Milliseconds => "ms",
			Self::Seconds => "s",
		}
	}

	pub fn encode_timestamp<W: io::Write>(&self, w: &mut W, ts: &DateTime<Utc>) -> io::Result<()> {
		// XXX: do something about leap seconds
		match self {
			Self::Seconds => write!(w, "{}", ts.timestamp()),
			Self::Milliseconds => {
				let ms = ts.timestamp_subsec_millis();
				let ms = if ms >= 999 {
					999
				} else {
					ms
				};
				write!(w, "{}{:03}", ts.timestamp(), ms)
			},
			Self::Microseconds => {
				let us = ts.timestamp_subsec_micros();
				let us = if us >= 999_999 {
					999_999
				} else {
					us
				};
				write!(w, "{}{:06}", ts.timestamp(), us)
			},
			Self::Nanoseconds => {
				let ns = ts.timestamp_subsec_nanos();
				let ns = if ns >= 999_999_999 {
					999_999_999
				} else {
					ns
				};
				write!(w, "{}{:09}", ts.timestamp(), ns)
			},
		}
	}
}

fn write_influx_sample<W: io::Write>(
		dest: &mut W,
		measurement: &str,
		tags: &[(&str, &str)],
		fields: &[(&str, f64)],
		timestamp: &DateTime<Utc>,
		precision: Precision) -> io::Result<()>
{
	// TODO: some proper escaping :>
	write!(dest, "{}", measurement)?;
	for (k, v) in tags.iter() {
		write!(dest, ",{}={}", k, v)?;
	}
	let mut first = true;
	for (k, v) in fields.iter() {
		write!(dest, "{}{}={:?}", if first { ' ' } else { ',' }, k, v)?;
		first = false;
	}
	write!(dest, " ")?;
	precision.encode_timestamp(dest, timestamp)?;
	write!(dest, "\n")?;
	Ok(())
}

pub enum Error {
	Request(reqwest::Error),
	PermissionError,
	DataError,
	DatabaseNotFound,
	UnexpectedSuccessStatus,
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Request(e) => fmt::Display::fmt(e, f),
			Self::PermissionError => write!(f, "permission denied"),
			Self::DataError => write!(f, "malformed data"),
			Self::DatabaseNotFound => write!(f, "database not found"),
			Self::UnexpectedSuccessStatus => write!(f, "unexpected success status"),
		}
	}
}

impl From<reqwest::Error> for Error {
	fn from(err: reqwest::Error) -> Self {
		Self::Request(err)
	}
}

pub struct Client {
	client: reqwest::Client,
	write_url: String,
	auth: Auth,
}

impl Client {
	pub fn new(api_url: String, auth: Auth) -> Self {
		Self{
			client: reqwest::Client::new(),
			write_url: format!("{}/write", api_url),
			auth,
		}
	}

	pub async fn post(
			&self,
			database: &'_ str,
			retention_policy: Option<&'_ str>,
			precision: Precision,
			auth: Option<&'_ Auth>,
			readout: &metric::Readout,
			) -> Result<(), Error>
	{
		let req = self.client.post(self.write_url.clone());
		let req = auth.unwrap_or_else(|| { &self.auth }).apply(req);
		let req = req.query(&[
			("db", database),
			("precision", precision.value()),
		]);
		let req = match retention_policy {
			Some(policy) => req.query(&[("rp", policy)]),
			None => req,
		};

		let body = BytesMut::new();
		let mut body_writer = body.writer();
		let mut fields = Vec::<(&str, f64)>::new();
		for (k, v) in readout.components.iter() {
			fields.push((&k, v.magnitude));
		}
		write_influx_sample(
			&mut body_writer,
			&readout.path.device_type,
			&[
				("instance", &readout.path.instance),
			],
			&fields[..],
			&readout.timestamp,
			precision,
		).unwrap();

		let body = body_writer.into_inner();
		let req = req.body(body.freeze());
		let resp = req.send().await?;
		match resp.error_for_status() {
			Ok(resp) => match resp.status() {
				reqwest::StatusCode::NO_CONTENT => Ok(()),
				_ => Err(Error::UnexpectedSuccessStatus),
			},
			Err(e) => match e.status().unwrap() {
				reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::UNAUTHORIZED => Err(Error::PermissionError),
				reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::PAYLOAD_TOO_LARGE => Err(Error::DataError),
				reqwest::StatusCode::NOT_FOUND => Err(Error::DatabaseNotFound),
				_ => Err(Error::Request(e)),
			},
		}
	}
}
