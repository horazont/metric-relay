use std::fmt;

#[derive(Debug, Clone)]
pub enum EvalError {
	ValueNotFound(String),
}

impl fmt::Display for EvalError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::ValueNotFound(ref s) => write!(f, "value not found in readout: {:?}", s),
		}
	}
}

#[derive(Debug, Clone)]
pub enum CompileError {
	UndeclaredFunction(String),
	InvalidToken(String),
	TooManyValues,
	StackUnderflow,
}

impl fmt::Display for CompileError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::UndeclaredFunction(ref s) => write!(f, "reference to undeclared function !{}", s),
			Self::InvalidToken(ref s) => write!(f, "invalid token {:?}", s),
			Self::TooManyValues => write!(f, "script returns too many values"),
			Self::StackUnderflow => write!(f, "not enough values on stack for operation"),
		}
	}
}

type StdResult<T, E> = core::result::Result<T, E>;
pub type EvalResult<T> = StdResult<T, EvalError>;
pub type CompileResult<T> = StdResult<T, CompileError>;
