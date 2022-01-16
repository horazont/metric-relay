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

type StdResult<T, E> = core::result::Result<T, E>;
pub type EvalResult<T> = StdResult<T, EvalError>;
