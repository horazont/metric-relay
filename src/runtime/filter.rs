use std::collections::HashMap;
use std::sync::Arc;
use smartstring::alias::{String as SmartString};
use log::{warn, trace};

use glob::{Pattern, MatchOptions};

use crate::metric;
use crate::script;

use super::payload;


pub trait Filter: Send + Sync {
	fn process_readout(&self, input: payload::Readout) -> Option<payload::Readout> {
		Some(input)
	}

	fn process_stream(&self, input: payload::Stream) -> Option<payload::Stream> {
		Some(input)
	}
}

pub struct SelectByPath {
	pub invert: bool,
	pub match_device_type: Option<Pattern>,
	pub match_instance: Option<Pattern>,
}

impl SelectByPath {
	fn matches_path(&self, path: &metric::DevicePath) -> bool {
		static MATCH_OPTIONS: MatchOptions = MatchOptions{
			case_sensitive: false,
			require_literal_separator: true,
			require_literal_leading_dot: false,
		};

		match self.match_device_type.as_ref() {
			Some(p) => if !p.matches_with(&path.device_type, MATCH_OPTIONS) {
				trace!("select by path rejected {:?} because the device type did not match {:?}", path, self.match_device_type);
				return self.invert;
			},
			None => (),
		};

		match self.match_instance.as_ref() {
			Some(p) => if !p.matches_with(&path.instance, MATCH_OPTIONS) {
				trace!("select by path rejected {:?} because the instance did not match {:?}", path, self.match_instance);
				return self.invert;
			},
			None => (),
		};

		trace!("select by path accepted {:?}", path);
		!self.invert
	}

	pub fn matches_readout(&self, readout: &metric::Readout) -> bool {
		self.matches_path(&readout.path)
	}

	pub fn matches_stream(&self, block: &metric::StreamBlock) -> bool {
		self.matches_path(&block.path)
	}
}

impl Filter for SelectByPath {
	fn process_readout(&self, input: payload::Readout) -> Option<payload::Readout> {
		match self.matches_readout(&input) {
			true => Some(input),
			false => None,
		}
	}

	fn process_stream(&self, input: payload::Stream) -> Option<payload::Stream> {
		match self.matches_stream(&input) {
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
	fn process_readout(&self, mut input: payload::Readout) -> Option<payload::Readout> {
		if !self.predicate.matches_readout(&input) {
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
		let input_mut = Arc::make_mut(&mut input);
		if !new_value.is_nan() {
			input_mut.components.insert(self.new_component.clone(), metric::Value{
				magnitude: new_value,
				unit: self.new_unit.clone(),
			});
		}
		Some(input)
	}
}

pub struct Map {
	pub predicate: SelectByPath,
	pub script: Arc<Box<dyn script::Evaluate>>,
	pub new_unit: metric::Unit,
}

struct SingletonNamespace<'x> {
	pub name: &'x str,
	pub value: f64,
}

impl<'x> script::Namespace for SingletonNamespace<'x> {
	fn lookup<'y>(&self, name: &'y str) -> Option<f64> {
		if self.name == name {
			Some(self.value)
		} else {
			None
		}
	}
}

impl Filter for Map {
	fn process_readout(&self, mut input: payload::Readout) -> Option<payload::Readout> {
		if !self.predicate.matches_readout(&input) {
			trace!("map skipping {:?} because it was rejected by the predicate", input);
			return Some(input)
		}

		let input_mut = Arc::make_mut(&mut input);
		let mut new_components = metric::OrderedVec::with_capacity(input_mut.components.len());
		for (k, v) in input_mut.components.drain(..) {
			let ns = SingletonNamespace{
				name: "value",
				value: v.magnitude,
			};
			let ctx = script::Context::new(script::BoxCow::<'_, dyn script::Namespace>::wrap_ref(&ns));

			let new_magnitude = match self.script.evaluate(&ctx) {
				Ok(v) => v,
				Err(e) => {
					warn!("failed to evaluate value: {}", e);
					drop(ctx);
					drop(ns);
					continue;
				},
			};
			drop(ctx);
			drop(ns);

			new_components.insert(k, metric::Value{
				magnitude: new_magnitude,
				unit: self.new_unit.clone(),
			});
		}
		std::mem::swap(&mut new_components, &mut input_mut.components);
		Some(input)
	}
}

pub struct DropComponent {
	pub predicate: SelectByPath,
	pub component_name: SmartString,
}

impl Filter for DropComponent {
	fn process_readout(&self, mut input: payload::Readout) -> Option<payload::Readout> {
		if !self.predicate.matches_readout(&input) {
			return Some(input)
		}

		if input.components.contains_key(&self.component_name) {
			let input_mut = Arc::make_mut(&mut input);
			input_mut.components.remove(&self.component_name);
		}
		Some(input)
	}
}

pub struct MapInstance {
	pub predicate: SelectByPath,
	pub mapping: HashMap<SmartString, SmartString>,
}

impl Filter for MapInstance {
	fn process_readout(&self, mut input: payload::Readout) -> Option<payload::Readout> {
		if !self.predicate.matches_readout(&input) {
			return Some(input)
		}
		match self.mapping.get(&input.path.instance) {
			None => return Some(input),
			Some(new) => {
				let input_mut = Arc::make_mut(&mut input);
				input_mut.path.instance = new.clone();
			},
		}

		Some(input)
	}

	fn process_stream(&self, mut input: payload::Stream) -> Option<payload::Stream> {
		if !self.predicate.matches_stream(&input) {
			return Some(input)
		}
		match self.mapping.get(&input.path.instance) {
			None => return Some(input),
			Some(new) => {
				let input_mut = Arc::make_mut(&mut input);
				input_mut.path.instance = new.clone();
			},
		}

		Some(input)
	}
}

pub struct MapDeviceType {
	pub predicate: SelectByPath,
	pub mapping: HashMap<SmartString, SmartString>,
}

impl Filter for MapDeviceType {
	fn process_readout(&self, mut input: payload::Readout) -> Option<payload::Readout> {
		if !self.predicate.matches_readout(&input) {
			return Some(input)
		}
		match self.mapping.get(&input.path.device_type) {
			None => return Some(input),
			Some(new) => {
				let input_mut = Arc::make_mut(&mut input);
				input_mut.path.device_type = new.clone();
			},
		}

		Some(input)
	}

	fn process_stream(&self, mut input: payload::Stream) -> Option<payload::Stream> {
		if !self.predicate.matches_stream(&input) {
			return Some(input)
		}
		match self.mapping.get(&input.path.device_type) {
			None => return Some(input),
			Some(new) => {
				let input_mut = Arc::make_mut(&mut input);
				input_mut.path.device_type = new.clone();
			},
		}

		Some(input)
	}
}

impl Filter for Vec<Box<dyn Filter>> {
	fn process_readout(&self, mut item: payload::Readout) -> Option<payload::Readout> {
		for filter in self.iter() {
			item = match filter.process_readout(item) {
				Some(item) => item,
				None => return None,
			}
		}
		Some(item)
	}

	fn process_stream(&self, mut item: payload::Stream) -> Option<payload::Stream> {
		for filter in self.iter() {
			item = match filter.process_stream(item) {
				Some(item) => item,
				None => return None,
			}
		}
		Some(item)
	}
}
