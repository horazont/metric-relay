use std::sync::Arc;
use crate::metric;

pub type Sample = Arc<metric::Readout>;
pub type Stream = Arc<metric::StreamBlock>;
