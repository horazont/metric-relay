use std::fmt;
use std::str::FromStr;

mod context;
mod func;
mod result;

pub use result::{EvalResult, EvalError, CompileResult, CompileError};
pub use context::{Evaluate, Context, Namespace, BoxCow};

#[derive(Debug)]
struct Constant(f64);

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
struct AddOp {
	lhs: Box<dyn Evaluate>,
	rhs: Box<dyn Evaluate>,
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
struct SubOp {
	lhs: Box<dyn Evaluate>,
	rhs: Box<dyn Evaluate>,
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
struct MulOp {
	lhs: Box<dyn Evaluate>,
	rhs: Box<dyn Evaluate>,
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
struct DivOp {
	lhs: Box<dyn Evaluate>,
	rhs: Box<dyn Evaluate>,
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
struct PowOp {
	lhs: Box<dyn Evaluate>,
	rhs: Box<dyn Evaluate>,
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
struct Ref(String);

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

struct TokenIter<'x>{
	s: &'x str,
}

impl<'x> Iterator for TokenIter<'x> {
	type Item = &'x str;

	fn next(&mut self) -> Option<&'x str> {
		// remove any whitespace
		self.s = match self.s.strip_prefix(&[' ', '\t', '\n', '\r'][..]) {
			Some(v) => v,
			None => self.s,
		};
		if self.s.len() == 0 {
			return None
		}

		let (token, remainder) = match self.s.find(&[' ', '\t', '\n', '\r'][..]) {
			Some(at) => self.s.split_at(at),
			None => (self.s, &self.s[..0]),
		};

		self.s = remainder;
		if token.len() > 0 {
			Some(token)
		} else {
			None
		}
	}
}

#[derive(Debug, Clone)]
enum Operator {
	Add,
	Sub,
	Mul,
	Div,
	Pow,
}

#[derive(Debug, Clone)]
enum Token {
	Number(f64),
	FunctionCall(String),
	Operation(Operator),
	Reference(String),
}

fn parse_token(s: &str) -> CompileResult<Token> {
	assert!(s.len() > 0);
	let first_char = s.chars().next().unwrap();
	match first_char {
		'0'..='9' => match s.parse::<f64>() {
			Ok(v) => Ok(Token::Number(v)),
			Err(_) => Err(CompileError::InvalidToken(s.into())),
		},
		'!' => if s.len() > 1 {
			Ok(Token::FunctionCall(s[1..].into()))
		} else {
			Err(CompileError::InvalidToken(s.into()))
		},
		'+' if s.len() == 1 => Ok(Token::Operation(Operator::Add)),
		'-' if s.len() == 1 => Ok(Token::Operation(Operator::Sub)),
		'-' => match s.parse::<f64>() {
			Ok(v) => Ok(Token::Number(v)),
			Err(_) => Err(CompileError::InvalidToken(s.into())),
		},
		'*' if s.len() == 1 => Ok(Token::Operation(Operator::Mul)),
		'/' if s.len() == 1 => Ok(Token::Operation(Operator::Div)),
		'^' if s.len() == 1 => Ok(Token::Operation(Operator::Pow)),
		_ => Ok(Token::Reference(s.into())),
	}
}

impl FromStr for Box<dyn Evaluate> {
	type Err = CompileError;

	fn from_str(s: &str) -> CompileResult<Box<dyn Evaluate>> {
		use Token::*;
		use Operator::*;

		let mut stack = Vec::<Box<dyn Evaluate>>::new();
		for token in (TokenIter{s}) {
			match parse_token(token)? {
				Number(v) => stack.push(Box::new(Constant(v))),
				Operation(Add) => if stack.len() < 2 {
					return Err(CompileError::StackUnderflow)
				} else {
					let rhs = stack.pop().unwrap();
					let lhs = stack.pop().unwrap();
					stack.push(Box::new(AddOp{lhs, rhs}))
				},
				Operation(Mul) => if stack.len() < 2 {
					return Err(CompileError::StackUnderflow)
				} else {
					let rhs = stack.pop().unwrap();
					let lhs = stack.pop().unwrap();
					stack.push(Box::new(MulOp{lhs, rhs}))
				},
				Operation(Sub) => if stack.len() < 2 {
					return Err(CompileError::StackUnderflow)
				} else {
					let rhs = stack.pop().unwrap();
					let lhs = stack.pop().unwrap();
					stack.push(Box::new(SubOp{lhs, rhs}))
				},
				Operation(Div) => if stack.len() < 2 {
					return Err(CompileError::StackUnderflow)
				} else {
					let rhs = stack.pop().unwrap();
					let lhs = stack.pop().unwrap();
					stack.push(Box::new(DivOp{lhs, rhs}))
				},
				Operation(Pow) => if stack.len() < 2 {
					return Err(CompileError::StackUnderflow)
				} else {
					let rhs = stack.pop().unwrap();
					let lhs = stack.pop().unwrap();
					stack.push(Box::new(PowOp{lhs, rhs}))
				},
				Reference(name) => stack.push(Box::new(Ref(name))),
				FunctionCall(name) => match &name[..] {
					"heat_index" => {
						if stack.len() < 2 {
							return Err(CompileError::StackUnderflow)
						} else {
							let hum = stack.pop().unwrap();
							let temp = stack.pop().unwrap();
							stack.push(Box::new(func::F2Op{
								p1: temp,
								p2: hum,
								f: func::heat_index_wrap,
								label: "heat_index",
							}))
						}
					},
					"dewpoint" => {
						if stack.len() < 2 {
							return Err(CompileError::StackUnderflow)
						} else {
							let hum = stack.pop().unwrap();
							let temp = stack.pop().unwrap();
							stack.push(Box::new(func::F2Op{
								p1: temp,
								p2: hum,
								f: func::dewpoint_wrap,
								label: "dewpoint",
							}))
						}
					},
					"wet_bulb_temperature" => {
						if stack.len() < 2 {
							return Err(CompileError::StackUnderflow)
						} else {
							let hum = stack.pop().unwrap();
							let temp = stack.pop().unwrap();
							stack.push(Box::new(func::F2Op{
								p1: temp,
								p2: hum,
								f: func::wet_bulb_temperature_wrap,
								label: "wet_bulb_temperature",
							}))
						}
					},
					"barometric_correction" => {
						if stack.len() < 5 {
							return Err(CompileError::StackUnderflow)
						} else {
							let height = stack.pop().unwrap();
							let g_0 = stack.pop().unwrap();
							let humidity = stack.pop().unwrap();
							let temperature = stack.pop().unwrap();
							let pressure = stack.pop().unwrap();
							stack.push(Box::new(func::BarometricCorrectionOp{
								pressure,
								temperature,
								humidity,
								g_0,
								height,
							}))
						}
					},
					"dB" => {
						if stack.len() < 2 {
							return Err(CompileError::StackUnderflow)
						} else {
							let limit = stack.pop().unwrap();
							let value = stack.pop().unwrap();
							stack.push(Box::new(func::F2Op{
								p1: value,
								p2: limit,
								f: func::to_decibel,
								label: "dB",
							}))
						}
					},
					_ => return Err(CompileError::UndeclaredFunction(name)),
				}
			}
		}
		if stack.len() > 1 {
			return Err(CompileError::TooManyValues)
		}
		match stack.pop() {
			Some(v) => Ok(v),
			None => Err(CompileError::StackUnderflow)
		}
	}
}

#[cfg(test)]
mod test_compile {
	use super::*;

	use std::collections::HashMap;

	#[test]
	fn test_constant() {
		let s = "23.42";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), 23.42f64);
	}

	#[test]
	fn test_negative_constant() {
		let s = "-23.42";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), -23.42f64);
	}

	#[test]
	fn test_addition() {
		let s = "10 20 +";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), 30f64);
	}

	#[test]
	fn test_subtraction() {
		let s = "10 20 -";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), -10f64);
	}

	#[test]
	fn test_multiplication() {
		let s = "10 20 *";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), 200f64);
	}

	#[test]
	fn test_division() {
		let s = "10 20 /";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), 0.5f64);
	}

	#[test]
	fn test_division_by_zero() {
		let s = "10 0 /";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert!(expr.evaluate(&ctx).unwrap().is_infinite());
	}

	#[test]
	fn test_power() {
		let s = "10 20 ^";
		let ctx = Context::empty();
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		assert_eq!(expr.evaluate(&ctx).unwrap(), 1e20f64);
	}

	#[test]
	fn test_reference() {
		let s = "x y +";
		let mut ns = HashMap::new();
		ns.insert("x", 10f64);
		ns.insert("y", 20f64);
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		{
			let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
			assert_eq!(expr.evaluate(&ctx).unwrap(), 30f64);
		}
		ns.insert("x", 30f64);
		{
			let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
			assert_eq!(expr.evaluate(&ctx).unwrap(), 50f64);
		}
	}

	#[test]
	fn test_reference_with_undeclared_name() {
		let s = "x y + z *";
		let mut ns = HashMap::new();
		ns.insert("x", 10f64);
		ns.insert("y", 20f64);
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
		match expr.evaluate(&ctx) {
			Err(EvalError::ValueNotFound(s)) => {
				assert_eq!(s, "z");
			},
			other => panic!("unexpected result: {:?}", other),
		}
	}

	#[test]
	fn test_complex_expression() {
		let s = "a b * 3.14159 r 2 ^ * *";
		let mut ns = HashMap::new();
		ns.insert("a", 10f64);
		ns.insert("b", 20f64);
		ns.insert("r", 3f64);
		let expr = s.parse::<Box<dyn Evaluate>>().unwrap();
		let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
		assert_eq!(expr.evaluate(&ctx).unwrap(), 10f64 * 20f64 * 3.14159 * 3f64 * 3f64);
	}
}
