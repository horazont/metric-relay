use std::fmt::Write;

use microtemplate::{Substitutions, render};
use xml::escape::{escape_str_attribute};

use crate::metric;

#[derive(Substitutions)]
struct TemplateArgs<'a> {
	instance: &'a str,
}

pub struct Client {
	client: reqwest::Client,
	api_url: String,
	node_template: String,
	override_host: Option<String>,
}

impl Client {
	pub fn new(api_url: String, node_template: String, override_host: Option<String>) -> Self {
		Self{
			client: reqwest::Client::new(),
			api_url,
			node_template,
			override_host,
		}
	}

	pub async fn post(
			&self,
			readout: &metric::Readout,
			) -> Result<(), reqwest::Error>
	{
		let mut payload = format!(
			"<sample-batch xmlns='https://xmlns.zombofant.net/hint/sensor/1.0'  timestamp='{}' part='{}' instance='{}'>",
			readout.timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
			escape_str_attribute(&readout.path.device_type),
			escape_str_attribute(&readout.path.instance),
		);

		for (k, v) in readout.components.iter() {
			write!(payload, "<numeric subpart='{}' value='{:?}'/>", escape_str_attribute(k), v.magnitude).unwrap();
		}
		payload.write_str("</sample-batch>").unwrap();

		let node = render(&self.node_template, TemplateArgs{instance: &readout.path.instance});

		let req = self.client.post(format!("{}/{}", self.api_url, node));
		let req = req.header("Content-Type", "application/xml");
		let req = match self.override_host.as_ref() {
			Some(v) => req.header("Host", v),
			None => req,
		};
		let req = req.body(payload);

		let resp = req.send().await?;
		resp.error_for_status()?;
		Ok(())
	}
}
