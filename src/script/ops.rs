use std::fmt;

use super::context::{Context, Evaluate};
use super::result::{EvalError, EvalResult};

pub const FTRUE: f64 = 1.0;
pub const FFALSE: f64 = 0.0;

pub fn is_float_true(f: f64) -> bool {
	f > 0.0
}

#[derive(Debug)]
pub(super) struct Constant(pub f64);

impl fmt::Display for Constant {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl Evaluate for Constant {
	fn evaluate<'x>(&self, _ctx: &'x Context) -> EvalResult<f64> {
		Ok(self.0)
	}
}

#[derive(Debug)]
pub(super) struct AddOp {
	pub lhs: Box<dyn Evaluate>,
	pub rhs: Box<dyn Evaluate>,
}

impl fmt::Display for AddOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} +", self.lhs, self.rhs)
	}
}

impl Evaluate for AddOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		Ok(self.lhs.evaluate(ctx)? + self.rhs.evaluate(ctx)?)
	}
}

#[derive(Debug)]
pub(super) struct SubOp {
	pub lhs: Box<dyn Evaluate>,
	pub rhs: Box<dyn Evaluate>,
}

impl fmt::Display for SubOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} +", self.lhs, self.rhs)
	}
}

impl Evaluate for SubOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		Ok(self.lhs.evaluate(ctx)? - self.rhs.evaluate(ctx)?)
	}
}

#[derive(Debug)]
pub(super) struct MulOp {
	pub lhs: Box<dyn Evaluate>,
	pub rhs: Box<dyn Evaluate>,
}

impl fmt::Display for MulOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} +", self.lhs, self.rhs)
	}
}

impl Evaluate for MulOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		Ok(self.lhs.evaluate(ctx)? * self.rhs.evaluate(ctx)?)
	}
}

#[derive(Debug)]
pub(super) struct DivOp {
	pub lhs: Box<dyn Evaluate>,
	pub rhs: Box<dyn Evaluate>,
}

impl fmt::Display for DivOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} +", self.lhs, self.rhs)
	}
}

impl Evaluate for DivOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		Ok(self.lhs.evaluate(ctx)? / self.rhs.evaluate(ctx)?)
	}
}

#[derive(Debug)]
pub(super) struct PowOp {
	pub lhs: Box<dyn Evaluate>,
	pub rhs: Box<dyn Evaluate>,
}

impl fmt::Display for PowOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{} {} +", self.lhs, self.rhs)
	}
}

impl Evaluate for PowOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		Ok(self.lhs.evaluate(ctx)?.powf(self.rhs.evaluate(ctx)?))
	}
}

#[derive(Debug)]
pub(super) struct LogicalAndOp(pub Vec<Box<dyn Evaluate>>);

impl fmt::Display for LogicalAndOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		for (i, op) in self.0.iter().enumerate() {
			if i > 0 {
				f.write_str(" ")?;
			}
			write!(f, "{}", op)?;
		}
		write!(f, "&&")
	}
}

impl Evaluate for LogicalAndOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		for op in self.0.iter() {
			if !is_float_true(op.evaluate(ctx)?) {
				return Ok(FFALSE);
			}
		}
		Ok(FTRUE)
	}
}

#[derive(Debug)]
pub(super) struct LogicalOrOp(pub Vec<Box<dyn Evaluate>>);

impl fmt::Display for LogicalOrOp {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		for (i, op) in self.0.iter().enumerate() {
			if i > 0 {
				f.write_str(" ")?;
			}
			write!(f, "{}", op)?;
		}
		write!(f, "||")
	}
}

impl Evaluate for LogicalOrOp {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		for op in self.0.iter() {
			if is_float_true(op.evaluate(ctx)?) {
				return Ok(FTRUE);
			}
		}
		Ok(FFALSE)
	}
}

#[derive(Debug)]
pub(super) struct Ref(pub String);

impl fmt::Display for Ref {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl Evaluate for Ref {
	fn evaluate<'x>(&self, ctx: &'x Context) -> EvalResult<f64> {
		match ctx.lookup(&self.0[..]) {
			Some(v) => Ok(v),
			None => Err(EvalError::ValueNotFound(self.0.clone())),
		}
	}
}
