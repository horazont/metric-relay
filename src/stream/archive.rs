use std::fmt;
use std::io;

use crate::metric;


#[derive(Debug)]
pub enum ArchiveError {
	IO(io::Error),
	IncompatiblePeriod,
}

impl From<io::Error> for ArchiveError {
	fn from(e: io::Error) -> Self {
		Self::IO(e)
	}
}

impl fmt::Display for ArchiveError {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::IO(e) => write!(f, "i/o error: {}", e),
			Self::IncompatiblePeriod => f.write_str("incompatible period"),
		}
	}
}

impl std::error::Error for ArchiveError {}

pub trait ArchiveWrite {
	fn write(&mut self, block: &metric::StreamBlock) -> Result<(), ArchiveError>;
}
