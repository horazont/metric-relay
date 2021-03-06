use std::sync::Arc;

use log::warn;

use smartstring::alias::String as SmartString;

use glob::{MatchOptions, Pattern};

#[cfg(feature = "regex")]
use regex::Regex;

use super::readout::{Readout, Sample};

pub trait Filter: Send + Sync {
	fn process(&self, readout: Arc<Readout>) -> Option<Arc<Readout>>;
}

pub struct Select {
	pub invert: bool,
	pub match_measurement: Option<Pattern>,
}

impl Select {
	pub fn matches(&self, readout: &Readout) -> bool {
		static MATCH_OPTIONS: MatchOptions = MatchOptions {
			case_sensitive: false,
			require_literal_separator: true,
			require_literal_leading_dot: false,
		};

		match self.match_measurement.as_ref() {
			Some(p) => {
				if !p.matches_with(&readout.measurement, MATCH_OPTIONS) {
					return self.invert;
				}
			}
			None => (),
		};

		!self.invert
	}
}

impl Default for Select {
	fn default() -> Self {
		Self {
			invert: false,
			match_measurement: None,
		}
	}
}

impl Filter for Select {
	fn process(&self, readout: Arc<Readout>) -> Option<Arc<Readout>> {
		if self.matches(&readout) {
			Some(readout)
		} else {
			None
		}
	}
}

pub struct Transpose {
	pub predicate: Select,
	pub tag: SmartString,
	pub field: SmartString,
}

impl Filter for Transpose {
	fn process(&self, mut readout: Arc<Readout>) -> Option<Arc<Readout>> {
		if !self.predicate.matches(&readout) {
			return Some(readout);
		}

		let mut new_samples = Vec::with_capacity(
			match readout.samples.len().checked_mul(readout.fields.len()) {
				Some(v) => v,
				None => {
					warn!("overflow while trying to transpose sample, dropping");
					return None;
				}
			},
		);

		let readout_mut = Arc::make_mut(&mut readout);
		readout_mut.tags.push(self.tag.clone());

		for mut sample in readout_mut.samples.drain(..) {
			if sample.fieldv.len() != readout_mut.fields.len() {
				warn!("dropping malformed sample (incorrect field count)");
				continue;
			}

			for (field, value) in readout_mut.fields.iter().zip(sample.fieldv.drain(..)) {
				let mut tagv = sample.tagv.clone();
				tagv.push(field.clone());
				new_samples.push(Sample {
					tagv,
					fieldv: vec![value],
				});
			}
		}

		std::mem::swap(&mut new_samples, &mut readout_mut.samples);
		readout_mut.fields.clear();
		readout_mut.fields.push(self.field.clone());
		Some(readout)
	}
}

#[cfg(feature = "regex")]
pub struct TagToField {
	pub predicate: Select,
	pub expr: Regex,
	pub new_tag_value: SmartString,
	pub field_name: SmartString,
}

#[cfg(feature = "regex")]
impl Filter for TagToField {
	fn process(&self, mut readout: Arc<Readout>) -> Option<Arc<Readout>> {
		if !self.predicate.matches(&readout) {
			return Some(readout);
		}

		if readout.fields.len() > 1 {
			warn!("TagToField can only be used with single-field readouts; dropping");
			return None;
		}

		if readout.tags.len() != 1 {
			warn!("TagToField only supports samples with a single tag; dropping");
			return None;
		}

		let readout_mut = Arc::make_mut(&mut readout);
		readout_mut.fields.clear();
		readout_mut.fields.reserve(readout_mut.samples.len());
		let mut new_fieldv = Vec::with_capacity(readout_mut.samples.len());
		let mut new_tagv = Vec::with_capacity(1);

		for sample in readout_mut.samples.drain(..) {
			let field_name = self
				.expr
				.replace(&sample.tagv[0][..], &self.field_name[..])
				.into();
			if new_tagv.len() == 0 {
				new_tagv.push(
					self.expr
						.replace(&sample.tagv[0][..], &self.new_tag_value[..])
						.into(),
				);
			}
			readout_mut.fields.push(field_name);
			new_fieldv.push(sample.fieldv[0]);
		}

		readout_mut.samples.push(Sample {
			fieldv: new_fieldv,
			tagv: new_tagv,
		});

		Some(readout)
	}
}

impl Filter for Vec<Box<dyn Filter>> {
	fn process(&self, mut readout: Arc<Readout>) -> Option<Arc<Readout>> {
		for filter in self.iter() {
			readout = filter.process(readout)?;
		}
		Some(readout)
	}
}
