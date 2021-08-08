use std::sync::Arc;
use crate::metric;

pub type Readout = Arc<metric::Readout>;
pub type Sample = Vec<Arc<metric::Readout>>;
pub type Stream = Arc<metric::StreamBlock>;
