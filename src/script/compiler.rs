use std::fmt;

use super::ast::{Location, Node};
use super::context::Evaluate;
use super::func;
use super::ops::*;

#[derive(Debug, Clone)]
pub enum CompileError {
	BareList(Location),
	DynamicCall(Location),
	InvalidCall(Location),
	NotEnoughArguments(Location, String, usize),
	TooManyArguments(Location, String, usize),
	UndefinedFunction(Location, String),
	BareNil(Location),
}

impl fmt::Display for CompileError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::BareList(loc) => write!(f, "{}: bare list", loc),
			Self::DynamicCall(loc) => write!(f, "{}: dynamic calls not supported", loc),
			Self::InvalidCall(loc) => write!(f, "{}: cannot call lists or numbers", loc),
			Self::NotEnoughArguments(loc, fname, min) => write!(
				f,
				"{}: {:?} requires at least {} arguments",
				loc, fname, min
			),
			Self::TooManyArguments(loc, fname, max) => {
				write!(f, "{}: {:?} takes at most {} arguments", loc, fname, max)
			}
			Self::UndefinedFunction(loc, fname) => {
				write!(f, "{}: function {:?} not defined", loc, fname)
			}
			Self::BareNil(loc) => write!(f, "{}: nil encountered", loc),
		}
	}
}

pub type CompileResult<T> = Result<T, CompileError>;

fn sum(mut argv: Vec<Box<dyn Evaluate>>) -> Box<dyn Evaluate> {
	// TODO: support multi-arg AddOp
	let mut expr = argv.remove(0);
	for more in argv.drain(..) {
		expr = Box::new(AddOp {
			lhs: expr,
			rhs: more,
		})
	}
	expr
}

fn prod(mut argv: Vec<Box<dyn Evaluate>>) -> Box<dyn Evaluate> {
	// TODO: support multi-arg MulOp
	let mut expr = argv.remove(0);
	for more in argv.drain(..) {
		expr = Box::new(MulOp {
			lhs: expr,
			rhs: more,
		})
	}
	expr
}

trait Func {
	fn name(&self) -> &str;
	fn compile<'x>(
		&self,
		compiler: &Compiler,
		loc: Location,
		name: &str,
		argv: Vec<Node<'x>>,
	) -> CompileResult<Box<dyn Evaluate>>;
}

impl<T: Func> Func for &'static T {
	fn name(&self) -> &str {
		<T as Func>::name(*self)
	}

	fn compile<'x>(
		&self,
		compiler: &Compiler,
		loc: Location,
		name: &str,
		argv: Vec<Node<'x>>,
	) -> CompileResult<Box<dyn Evaluate>> {
		<T as Func>::compile(*self, compiler, loc, name, argv)
	}
}

struct Alias<T> {
	name: &'static str,
	inner: T,
}

impl<T> Alias<T> {
	const fn new(name: &'static str, inner: T) -> Self {
		Self { name, inner }
	}
}

impl<T: Func> Func for Alias<T> {
	fn name(&self) -> &str {
		self.name
	}

	fn compile<'x>(
		&self,
		compiler: &Compiler,
		loc: Location,
		name: &str,
		argv: Vec<Node<'x>>,
	) -> CompileResult<Box<dyn Evaluate>> {
		self.inner.compile(compiler, loc, name, argv)
	}
}

macro_rules! func {
	($(impl Func for $sname:ident as $($sstaticname:ident<$name:literal>),+ { $compile:item })+) => {
		$(
			#[derive(Clone, Copy, Debug)]
			pub struct $sname();

			impl crate::script::compiler::Func for $sname {
				fn name(&self) -> &str {
					"(unnamed)"
				}

				$compile
			}

			$(
				#[allow(dead_code)]
				static $sstaticname: Alias<$sname> = Alias::new($name, $sname());
			)+
		)+
	};
}

func! {
	impl Func for AddFunc as ADD<"+">, NAMED_ADD<"add"> {
		fn compile<'x>(
				&self,
				compiler: &Compiler,
				loc: Location,
				name: &str,
				argv: Vec<Node<'x>>,
		) -> CompileResult<Box<dyn Evaluate>> {
			if argv.len() < 1 {
				return Err(CompileError::NotEnoughArguments(loc, name.into(), 1))
			}
			let argv = compiler.compile_as_exprs(argv)?;
			Ok(sum(argv))
		}
	}

	impl Func for SubFunc as SUB<"-">, NAMED_SUB<"sub"> {
		fn compile<'x>(
				&self,
				compiler: &Compiler,
				loc: Location,
				name: &str,
				argv: Vec<Node<'x>>,
		) -> CompileResult<Box<dyn Evaluate>> {
			if argv.len() < 2 {
				return Err(CompileError::NotEnoughArguments(loc, name.into(), 2))
			}
			let mut argv = compiler.compile_as_exprs(argv)?;
			let lhs = argv.remove(0);
			Ok(Box::new(SubOp{
				lhs,
				rhs: sum(argv),
			}))
		}
	}

	impl Func for MulFunc as MUL<"*">, NAMED_MUL<"mul"> {
		fn compile<'x>(
				&self,
				compiler: &Compiler,
				loc: Location,
				name: &str,
				argv: Vec<Node<'x>>,
		) -> CompileResult<Box<dyn Evaluate>> {
			if argv.len() < 1 {
				return Err(CompileError::NotEnoughArguments(loc, name.into(), 1))
			}
			let argv = compiler.compile_as_exprs(argv)?;
			Ok(prod(argv))
		}
	}

	impl Func for DivFunc as DIV<"/">, NAMED_DIV<"div"> {
		fn compile<'x>(
				&self,
				compiler: &Compiler,
				loc: Location,
				name: &str,
				argv: Vec<Node<'x>>,
		) -> CompileResult<Box<dyn Evaluate>> {
			if argv.len() < 2 {
				return Err(CompileError::NotEnoughArguments(loc, name.into(), 2))
			}
			let mut argv = compiler.compile_as_exprs(argv)?;
			let lhs = argv.remove(0);
			Ok(Box::new(DivOp{
				lhs,
				rhs: prod(argv),
			}))
		}
	}

	impl Func for PowFunc as POW<"^">, NAMED_POW<"pow"> {
		fn compile<'x>(
				&self,
				compiler: &Compiler,
				loc: Location,
				name: &str,
				argv: Vec<Node<'x>>,
		) -> CompileResult<Box<dyn Evaluate>> {
			if argv.len() < 2 {
				return Err(CompileError::NotEnoughArguments(loc, name.into(), 2))
			}
			if argv.len() > 2 {
				return Err(CompileError::TooManyArguments(loc, name.into(), 2))
			}
			let mut argv = compiler.compile_as_exprs(argv)?;
			let lhs = argv.remove(0);
			let rhs = argv.remove(0);
			Ok(Box::new(PowOp{ lhs, rhs }))
		}
	}

	impl Func for BarometricCorrectionFunc as BAROMETRIC_CORRECTION<"barometric-correction"> {
		fn compile<'x>(
				&self,
				compiler: &Compiler,
				loc: Location,
				name: &str,
				argv: Vec<Node<'x>>,
		) -> CompileResult<Box<dyn Evaluate>> {
			if argv.len() < 5 {
				return Err(CompileError::NotEnoughArguments(loc, name.into(), 5))
			}
			if argv.len() > 5 {
				return Err(CompileError::TooManyArguments(loc, name.into(), 5))
			}
			let mut argv = compiler.compile_as_exprs(argv)?;
			let pressure = argv.remove(0);
			let temperature = argv.remove(0);
			let humidity = argv.remove(0);
			let g_0 = argv.remove(0);
			let height = argv.remove(0);
			Ok(Box::new(func::BarometricCorrectionOp {
				pressure,
				temperature,
				humidity,
				g_0,
				height,
			}))
		}
	}
}

macro_rules! f2func {
	($(impl Func for $sname:ident as $($sstaticname:ident<$name:literal>),+ use $f:expr; )+) => {
		$(
			#[derive(Clone, Copy, Debug)]
			pub struct $sname();

			impl crate::script::compiler::Func for $sname {
				fn name(&self) -> &str {
					"(unnamed)"
				}

				fn compile<'x>(
						&self,
						compiler: &Compiler,
						loc: Location,
						name: &str,
						argv: Vec<Node<'x>>,
				) -> CompileResult<Box<dyn Evaluate>> {
					if argv.len() < 2 {
						return Err(CompileError::NotEnoughArguments(loc, name.into(), 2))
					}
					if argv.len() > 2 {
						return Err(CompileError::TooManyArguments(loc, name.into(), 2))
					}
					let mut argv = compiler.compile_as_exprs(argv)?;
					let lhs = argv.remove(0);
					let rhs = argv.remove(0);
					Ok(Box::new(func::F2Op{ p1: lhs, p2: rhs, f: $f, label: stringify!($sname) }))
				}
			}

			$(
				#[allow(dead_code)]
				static $sstaticname: Alias<$sname> = Alias::new($name, $sname());
			)+
		)+
	}
}

f2func! {
	impl Func for HeatIndexFunc as HEAT_INDEX<"heat-index"> use func::heat_index_wrap;
	impl Func for DewpointFunc as DEWPOINT<"dewpoint"> use func::dewpoint_wrap;
	impl Func for WetBulbTemperatureFunc as WET_BULB_TEMPERATURE<"wet-bulb-temperature"> use func::wet_bulb_temperature_wrap;
	impl Func for DecibelFunc as TO_DECIBEL<"to-decibel"> use func::to_decibel;
}

macro_rules! funcs {
	($($name:expr,)+) => {
		vec![
			$(&$name as &dyn Func,)+
		]
	};
	(use default $($name:expr,)*) => {
		funcs!(
			$($name,)*
			crate::script::compiler::ADD,
			crate::script::compiler::NAMED_ADD,
			crate::script::compiler::SUB,
			crate::script::compiler::NAMED_SUB,
			crate::script::compiler::DIV,
			crate::script::compiler::NAMED_DIV,
			crate::script::compiler::MUL,
			crate::script::compiler::NAMED_MUL,
			crate::script::compiler::POW,
			crate::script::compiler::NAMED_POW,
			crate::script::compiler::HEAT_INDEX,
			crate::script::compiler::DEWPOINT,
			crate::script::compiler::WET_BULB_TEMPERATURE,
			crate::script::compiler::TO_DECIBEL,
			crate::script::compiler::BAROMETRIC_CORRECTION,
		)
	};
}

pub struct Compiler {
	funcs: Vec<&'static dyn Func>,
}

impl Default for Compiler {
	fn default() -> Self {
		Self::new(funcs!(use default))
	}
}

impl Compiler {
	fn new(funcs: Vec<&'static dyn Func>) -> Self {
		Self { funcs }
	}

	pub fn compile_as_exprs(
		&self,
		mut nodes: Vec<Node<'_>>,
	) -> CompileResult<Vec<Box<dyn Evaluate>>> {
		let mut result = Vec::new();
		for node in nodes.drain(..) {
			result.push(self.compile(node)?);
		}
		Ok(result)
	}

	pub fn compile(&self, node: Node<'_>) -> CompileResult<Box<dyn Evaluate>> {
		match node {
			Node::Number(_loc, v) => Ok(Box::new(Constant(v))),
			Node::Ref(_loc, s) => Ok(Box::new(Ref(s.to_string()))),
			Node::Nil(loc) => Err(CompileError::BareNil(loc)),
			Node::Call(start, _end, to_call, argv) => {
				let (loc, funcname) = match *to_call {
					Node::Ref(loc, s) => (loc, s),
					Node::Call(loc, ..) => return Err(CompileError::DynamicCall(loc)),
					_ => return Err(CompileError::InvalidCall(start)),
				};
				// TODO: maybe hashmap this?
				for func in self.funcs.iter() {
					if func.name() == funcname {
						return func.compile(self, loc, funcname, argv);
					}
				}
				Err(CompileError::UndefinedFunction(loc, funcname.into()))
			}
			Node::List(start, _end, ..) => Err(CompileError::BareList(start)),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;

	use super::super::ast;
	use super::super::context::{BoxCow, Context, Namespace};
	use super::super::result::EvalError;

	fn compile(s: &str) -> Box<dyn Evaluate> {
		let lexer = ast::Lexer::new(s);
		let node = Node::parse(lexer).unwrap();
		let compiler = Compiler::default();
		compiler.compile(node).unwrap()
	}

	#[test]
	fn constant() {
		let ctx = Context::empty();
		let expr = compile("23.42");
		assert_eq!(expr.evaluate(&ctx).unwrap(), 23.42);
	}

	#[test]
	fn negative_constant() {
		let ctx = Context::empty();
		let expr = compile("-23.42");
		assert_eq!(expr.evaluate(&ctx).unwrap(), -23.42);
	}

	#[test]
	fn constant_addition() {
		let ctx = Context::empty();
		let expr = compile("(+ 1 2)");
		assert_eq!(expr.evaluate(&ctx).unwrap(), 3f64);
	}

	#[test]
	fn constant_naddition() {
		let ctx = Context::empty();
		let expr = compile("(+ 1 2 3 4 5 6)");
		assert_eq!(expr.evaluate(&ctx).unwrap(), 21f64);
	}

	#[test]
	fn constant_nsubtraction() {
		let ctx = Context::empty();
		let expr = compile("(- 6 5 4 3 2 1)");
		assert_eq!(expr.evaluate(&ctx).unwrap(), -9f64);
	}

	#[test]
	fn constant_nmultiplication() {
		let ctx = Context::empty();
		let expr = compile("(* 1 2 3 4 5)");
		assert_eq!(expr.evaluate(&ctx).unwrap(), 120f64);
	}

	#[test]
	fn constant_ndivision() {
		let ctx = Context::empty();
		let expr = compile("(/ 1 2 4 8)");
		assert_eq!(expr.evaluate(&ctx).unwrap(), 0.015625f64);
	}

	#[test]
	fn constant_pow() {
		let ctx = Context::empty();
		let expr = compile("(^ 2 3)");
		assert_eq!(expr.evaluate(&ctx).unwrap(), 8f64);
	}

	#[test]
	fn reference() {
		let expr = compile("(+ x y z)");
		let mut ns = HashMap::new();
		ns.insert("x", 10f64);
		ns.insert("y", 20f64);
		ns.insert("z", 40f64);
		{
			let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
			assert_eq!(expr.evaluate(&ctx).unwrap(), 70f64);
		}
		ns.insert("x", 30f64);
		{
			let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
			assert_eq!(expr.evaluate(&ctx).unwrap(), 90f64);
		}
	}

	#[test]
	fn reference_with_undeclared_name() {
		let expr = compile("(* (+ x y) z)");
		let mut ns = HashMap::new();
		ns.insert("x", 10f64);
		ns.insert("y", 20f64);
		let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
		match expr.evaluate(&ctx) {
			Err(EvalError::ValueNotFound(s)) => {
				assert_eq!(s, "z");
			}
			other => panic!("unexpected result: {:?}", other),
		}
	}

	#[test]
	fn complex_expression() {
		let expr = compile("(* a b (^ r 2) 3.14159)");
		let mut ns = HashMap::new();
		ns.insert("a", 10f64);
		ns.insert("b", 20f64);
		ns.insert("r", 3f64);
		let ctx = Context::new(BoxCow::<'_, dyn Namespace>::wrap_ref(&ns));
		assert_eq!(
			expr.evaluate(&ctx).unwrap(),
			10f64 * 20f64 * 3.14159 * 3f64 * 3f64
		);
	}
}
