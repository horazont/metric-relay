use crate::metric;
use std::sync::Arc;

pub type Readout = Arc<metric::Readout>;
pub type Sample = Vec<Arc<metric::Readout>>;
pub type Stream = Arc<metric::StreamBlock>;
