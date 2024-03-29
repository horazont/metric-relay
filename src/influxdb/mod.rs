use std::fmt;
use std::sync::Arc;

use log::trace;

use base64::Engine;
use bytes::{BufMut, BytesMut};
use reqwest;

use serde_derive::{Deserialize, Serialize};

mod filter;
mod readout;

#[cfg(feature = "regex")]
pub use filter::TagToField;
pub use filter::{Filter, Select, Transpose};
pub use readout::{Precision, Readout};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Auth {
	None,
	HTTP { username: String, password: String },
	Query { username: String, password: String },
}

impl Auth {
	pub fn apply(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
		match self {
			Self::None => req,
			Self::HTTP { username, password } => req.header(
				"Authorization",
				format!(
					"Basic {}",
					base64::engine::general_purpose::STANDARD
						.encode(format!("{}:{}", username, password,))
				),
			),
			Self::Query { username, password } => req.query(&[("u", username), ("p", password)]),
		}
	}
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
		Self {
			client: reqwest::Client::new(),
			write_url: format!("{}/write", api_url),
			auth,
		}
	}

	pub async fn post(
		&self,
		database: &'_ str,
		retention_policy: Option<&'_ str>,
		auth: Option<&'_ Auth>,
		precision: Precision,
		readouts: &[Arc<Readout>],
	) -> Result<(), Error> {
		let req = self.client.post(self.write_url.clone());
		let req = auth.unwrap_or_else(|| &self.auth).apply(req);
		let req = req.query(&[("db", database), ("precision", precision.value())]);
		let req = match retention_policy {
			Some(policy) => req.query(&[("rp", policy)]),
			None => req,
		};

		let body = BytesMut::new();
		let mut body_writer = body.writer();
		trace!("serializing {} readouts", readouts.len());
		for readout in readouts {
			if precision != readout.precision {
				panic!("inconsistent precisions in readouts!")
			}
			readout.write(&mut body_writer).unwrap(); // BytesMut is infallible
		}

		let body = body_writer.into_inner();
		let req = req.body(body.freeze());
		let resp = req.send().await?;
		match resp.error_for_status() {
			Ok(resp) => match resp.status() {
				reqwest::StatusCode::NO_CONTENT => Ok(()),
				_ => Err(Error::UnexpectedSuccessStatus),
			},
			Err(e) => match e.status().unwrap() {
				reqwest::StatusCode::FORBIDDEN | reqwest::StatusCode::UNAUTHORIZED => {
					Err(Error::PermissionError)
				}
				reqwest::StatusCode::BAD_REQUEST | reqwest::StatusCode::PAYLOAD_TOO_LARGE => {
					Err(Error::DataError)
				}
				reqwest::StatusCode::NOT_FOUND => Err(Error::DatabaseNotFound),
				_ => Err(Error::Request(e)),
			},
		}
	}
}
