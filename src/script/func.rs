use std::fmt;

use super::context;
use super::result;

use crate::meteo;


pub struct F2Op<T: Fn(f64, f64) -> result::EvalResult<f64> + Send + Sync> {
	pub p1: Box<dyn context::Evaluate>,
	pub p2: Box<dyn context::Evaluate>,
	pub f: T,
	pub label: &'static str,
}

impl<T: Fn(f64, f64) -> result::EvalResult<f64> + Send + Sync> fmt::Display for F2Op<T> {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} !{}", self.p1, self.p2, self.label)
	}
}

impl<T: Fn(f64, f64) -> result::EvalResult<f64> + Send + Sync> fmt::Debug for F2Op<T> {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("F2Op<T>")
			.field("p1", &self.p1)
			.field("p2", &self.p2)
			.field("label", &self.label)
			.finish()
	}
}

impl<T: Fn(f64, f64) -> result::EvalResult<f64> + Send + Sync> context::Evaluate for F2Op<T> {
	fn evaluate<'x>(&self, ctx: &'x context::Context) -> result::EvalResult<f64> {
		(self.f)(self.p1.evaluate(ctx)?, self.p2.evaluate(ctx)?)
	}
}

pub fn heat_index_wrap(temp: f64, hum: f64) -> result::EvalResult<f64> {
	match meteo::heat_index(temp, hum) {
		Some(v) => Ok(v),
		None => Ok(f64::NAN),
	}
}

pub fn dewpoint_wrap(temp: f64, hum: f64) -> result::EvalResult<f64> {
	Ok(meteo::dewpoint(temp, hum))
}

pub fn wet_bulb_temperature_wrap(temp: f64, hum: f64) -> result::EvalResult<f64> {
	Ok(meteo::wet_bulb_temperature(temp, hum))
}

#[derive(Debug)]
pub struct ToDecibelOp {
	pub value: Box<dyn context::Evaluate>,
}

impl fmt::Display for ToDecibelOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} !dB", self.value)
	}
}

impl context::Evaluate for ToDecibelOp {
	fn evaluate<'x>(&self, ctx: &'x context::Context) -> result::EvalResult<f64> {
		Ok(self.value.evaluate(ctx)?.log10() * 10.0)
	}
}


#[derive(Debug)]
pub struct BarometricCorrectionOp {
	pub pressure: Box<dyn context::Evaluate>,
	pub temperature: Box<dyn context::Evaluate>,
	pub humidity: Box<dyn context::Evaluate>,
	pub g_0: Box<dyn context::Evaluate>,
	pub height: Box<dyn context::Evaluate>,
}

impl fmt::Display for BarometricCorrectionOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} {} {} {} !barometric_correction", self.pressure, self.temperature, self.humidity, self.g_0, self.height)
	}
}

impl context::Evaluate for BarometricCorrectionOp {
	fn evaluate<'x>(&self, ctx: &'x context::Context) -> result::EvalResult<f64> {
		Ok(meteo::barometric_correction(
			self.pressure.evaluate(ctx)?,
			self.temperature.evaluate(ctx)?,
			self.humidity.evaluate(ctx)?,
			self.g_0.evaluate(ctx)?,
			self.height.evaluate(ctx)?,
		))
	}
}
