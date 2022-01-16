use std::fmt;
use std::str::FromStr;

mod ast;
mod compiler;
mod context;
mod func;
mod ops;
mod result;

pub use ast::ParseError;
pub use compiler::CompileError;
pub use context::{BoxCow, Context, Evaluate, Namespace};
pub use result::{EvalError, EvalResult};

#[derive(Debug, Clone)]
pub enum Error {
	Parse(ParseError),
	Compile(CompileError),
}

impl From<ParseError> for Error {
	fn from(other: ParseError) -> Self {
		Self::Parse(other)
	}
}

impl From<CompileError> for Error {
	fn from(other: CompileError) -> Self {
		Self::Compile(other)
	}
}

impl fmt::Display for Error {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Parse(e) => fmt::Display::fmt(e, f),
			Self::Compile(e) => fmt::Display::fmt(e, f),
		}
	}
}

impl std::error::Error for Error {}

impl FromStr for Box<dyn Evaluate> {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let lexer = ast::Lexer::new(s);
		let ast = ast::Node::parse(lexer)?;
		let c = compiler::Compiler::default();
		Ok(c.compile(ast)?)
	}
}
