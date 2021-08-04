/*!
# Local buffer for streamed data

## On high-frequency streams and timestamps

The high frequency (>= 1Hz) streams gathered by the sensor block have one
common issue: They cannot be accurately timestamped by a real-time clock,
because no real-time clock with sufficient precision is available on the
MCU, but also because of unknown latencies between the sample acquisition
and sample processing.

However, we have an accurate serial number (16 bit) for each sample; more
specifically, the first sample in a batch of streamed samples is
timestamped with a serial number and all subsequent samples are known to
have been acquired continuously from the source.

The "current" sequence number is also provided by the MCU on status update
frames, which means that we can correlate it with the relative "uptime"
(16 bit) millisecond timestamp, which is in turn convertible into a RTC clock
timestamp.

This conversion via the RTCifier is not perfect; because it is based by
sampling a second-precision RTC with the millisecond-prescision uptime
timestamps (plus some jitter from the packet pipeline from the MCU to the
ESP8266), it takes quite a bit of uptime to get an accurate mapping. During
this time, there is significant drift of the clock. Once a useful
*/
use std::path;
use std::ffi;
use std::io;
use std::time::Duration;

use chrono::{DateTime, Utc};

use bincode;
use bincode::Options;

use log::warn;

use openat::Dir;

use crate::metric;


pub struct StreamBuffer {
	root: Dir,
	period: Option<Duration>,
	nsamples: usize,
	npoints: usize,
	blocks: Vec<RecordInfo>,
}

struct RecordInfo {
	filename: ffi::OsString,
	t0: DateTime<Utc>,
	period: Duration,
	nsamples: usize,
}

impl StreamBuffer {
	pub fn new<P: AsRef<path::Path>>(root: P) -> Result<Self, io::Error> {
		let mut result = Self{
			root: Dir::open(root.as_ref())?,
			period: None,
			nsamples: 0,
			npoints: 0,
			blocks: Vec::new(),
		};
		result.restore()?;
		Ok(result)
	}

	fn restore_file(&mut self, fname: &ffi::OsStr) -> Result<RecordInfo, bincode::Error> {
		let mut f = self.root.open_file(fname)?;
		let obj: metric::StreamBlock = bincode::DefaultOptions::new().with_little_endian().deserialize_from(f)?;
		Ok(RecordInfo{
			filename: fname.into(),
			t0: obj.t0,
			period: obj.period,
			nsamples: obj.data.len(),
		})
	}

	fn restore(&mut self) -> Result<(), io::Error>{
		self.blocks.clear();

		let mut total_samples = 0;
		for entry in self.root.list_self()? {
			let entry = match entry {
				Ok(entry) => entry,
				Err(e) => {
					warn!("failed to read directory entry: {}", e);
					continue;
				},
			};

			let info = match self.restore_file(entry.file_name()) {
				Ok(info) => info,
				Err(e) => {
					warn!("failed to restore buffered data in {:?}: {}", entry.file_name(), e);
					match self.root.remove_file(entry.file_name()) {
						Ok(_) => (),
						Err(e) => warn!("failed to delete corrupt data in {:?}: {}", entry.file_name(), e),
					};
					continue;
				}
			};

			total_samples += info.nsamples;
			self.blocks.push(info);
		};

		self.blocks.sort_by_key(|r| { r.t0 });
		if self.blocks.len() == 0 {
			self.nsamples = 0;
			self.npoints = 0;
			self.period = None;
			return Ok(())
		}

		let t_epoch = self.blocks[0].t0;
		let t_end = {
			let last_block = &self.blocks[self.blocks.len()-1];
			last_block.t0 + chrono::Duration::from_std(last_block.period * last_block.nsamples as u32).unwrap()
		};
		let t_span = t_end - t_epoch;
		// XXX: this breaks with periods >= 1s
		let points = t_span.num_milliseconds() as u32 / self.blocks[0].period.subsec_millis();
		self.npoints = points;
		self.nsamples = total_samples;
		self.period = Some(self.blocks[0].period);
		Ok(())
	}

	fn reset_period(&mut self, new_period: Duration) {

	}

	pub fn write(&mut self, block: metric::StreamBlock) {
		match self.period {
			Some(v) if v == block.period => (),
			_ => self.reset_period(block.period),
		};

		// TODO: find previous end and do sensible things
	}
}
