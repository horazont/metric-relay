use std::collections::HashMap;
use std::sync::Arc;
use smartstring::alias::{String as SmartString};
use log::{warn, trace};

use glob::{Pattern, MatchOptions};

use crate::metric;
use crate::script;

pub trait Filter: Send {
	fn process(&self, input: metric::Readout) -> Option<metric::Readout>;
}

pub struct SelectByPath {
	pub invert: bool,
	pub match_device_type: Option<Pattern>,
	pub match_instance: Option<Pattern>,
}

impl SelectByPath {
	pub fn matches(&self, readout: &metric::Readout) -> bool {
		static MATCH_OPTIONS: MatchOptions = MatchOptions{
			case_sensitive: false,
			require_literal_separator: true,
			require_literal_leading_dot: false,
		};

		match self.match_device_type.as_ref() {
			Some(p) => if !p.matches_with(&readout.path.device_type, MATCH_OPTIONS) {
				trace!("select by path rejected {:?} because the device type did not match {:?}", readout.path, self.match_device_type);
				return self.invert;
			},
			None => (),
		};

		match self.match_instance.as_ref() {
			Some(p) => if !p.matches_with(&readout.path.instance, MATCH_OPTIONS) {
				trace!("select by path rejected {:?} because the instance did not match {:?}", readout.path, self.match_instance);
				return self.invert;
			},
			None => (),
		};

		trace!("select by path accepted {:?}", readout.path);
		!self.invert
	}
}

impl Filter for SelectByPath {
	fn process(&self, input: metric::Readout) -> Option<metric::Readout> {
		match self.matches(&input) {
			true => Some(input),
			false => None,
		}
	}
}

impl Default for SelectByPath {
	fn default() -> Self {
		Self{
			invert: false,
			match_device_type: None,
			match_instance: None,
		}
	}
}

pub struct Calc {
	pub predicate: SelectByPath,
	pub script: Arc<Box<dyn script::Evaluate>>,
	pub new_component: SmartString,
	pub new_unit: metric::Unit,
}

impl script::Namespace for metric::OrderedVec<SmartString, metric::Value> {
	fn lookup<'x>(&self, name: &'x str) -> Option<f64> {
		self.get(name).and_then(|v| { Some(v.magnitude) })
	}
}

impl Filter for Calc {
	fn process(&self, mut input: metric::Readout) -> Option<metric::Readout> {
		if !self.predicate.matches(&input) {
			trace!("calc skipping {:?} because it was rejected by the predicate", input);
			return Some(input)
		}

		let ctx = script::Context::new(script::BoxCow::<'_, dyn script::Namespace>::wrap_ref(&input.components));
		let new_value = match self.script.evaluate(&ctx) {
			Ok(v) => v,
			Err(e) => {
				warn!("failed to evaluate value: {}", e);
				drop(ctx);
				return Some(input);
			},
		};
		drop(ctx);
		trace!("calc created value {} for {:?}", new_value, self.new_component);
		if !new_value.is_nan() {
			input.components.insert(self.new_component.clone(), metric::Value{
				magnitude: new_value,
				unit: self.new_unit.clone(),
			});
		}
		Some(input)
	}
}

pub struct DropComponent {
	pub predicate: SelectByPath,
	pub component_name: SmartString,
}

impl Filter for DropComponent {
	fn process(&self, mut input: metric::Readout) -> Option<metric::Readout> {
		if !self.predicate.matches(&input) {
			return Some(input)
		}

		input.components.remove(&self.component_name);
		Some(input)
	}
}

pub struct MapInstance {
	pub predicate: SelectByPath,
	pub mapping: HashMap<SmartString, SmartString>,
}

impl Filter for MapInstance {
	fn process(&self, mut input: metric::Readout) -> Option<metric::Readout> {
		if !self.predicate.matches(&input) {
			return Some(input)
		}
		input.path.instance = match self.mapping.get(&input.path.instance) {
			Some(new) => new.clone(),
			None => return Some(input),
		};
		Some(input)
	}
}

impl Filter for Vec<Box<dyn Filter>> {
	fn process(&self, mut item: metric::Readout) -> Option<metric::Readout> {
		for filter in self.iter() {
			item = match filter.process(item) {
				Some(item) => item,
				None => return None,
			}
		}
		Some(item)
	}
}
