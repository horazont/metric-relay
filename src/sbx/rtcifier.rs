use std::collections::VecDeque;
use std::fmt::Debug;

use log::warn;

use chrono::{DateTime, Utc, Duration};

/// Map a local high-range, high-precision counter to a remote low-range counter of the same precision.
#[derive(Debug)]
pub struct Timeline {
	remote_tip: u16,
	local_tip: i64,
	slack: i32,
}

impl Timeline {
	pub fn new(slack: u16) -> Timeline {
		Timeline{
			remote_tip: 0,
			local_tip: 0,
			slack: slack as i32,
		}
	}

	fn wraparound_aware_minus(&self, v1: u16, v2: u16) -> i32 {
		let fwd_diff = v1.wrapping_sub(v2) as i32;
		let back_diff = v2.wrapping_sub(v1) as i32;

		if back_diff < self.slack {
			return -back_diff
		}

		return fwd_diff
	}

	pub fn feed_and_transform(&mut self, remote: u16) -> i64 {
		let change = self.wraparound_aware_minus(remote, self.remote_tip);
		self.local_tip = self.local_tip.checked_add(change as i64).expect("should've reset in between *shrug*");
		self.remote_tip = remote;
		self.local_tip
	}

	pub fn reset(&mut self, new_remote_tip: u16) {
		self.remote_tip = new_remote_tip;
		self.local_tip = 0;
	}

	#[cfg(test)]
	pub fn forward(&mut self, offset: i64) {
		assert!(offset >= 0);
		self.local_tip = self.local_tip.checked_add(offset).expect("should've reset before");
		self.remote_tip = self.remote_tip.wrapping_add(offset as u16);
	}
}

#[cfg(test)]
mod tests_timeline {
	use super::*;

	fn new_tl() -> Timeline {
		Timeline::new(1000)
	}

	#[test]
	fn test_feed_and_transform_monotonically() {
		let mut tl = new_tl();
		for i in std::iter::successors(Some(0), |&prev| { let next = prev + 100; if next < 65536 { Some(next) } else { None } }) {
			let i_16 = i as u16;
			assert_eq!(i, tl.feed_and_transform(i_16));
		}
	}

	#[test]
	fn test_feed_and_transform_wraparound() {
		let mut tl = new_tl();
		tl.feed_and_transform(0);
		tl.feed_and_transform(10000);
		tl.feed_and_transform(20000);
		tl.feed_and_transform(30000);
		tl.feed_and_transform(40000);
		tl.feed_and_transform(50000);
		tl.feed_and_transform(60000);
		assert_eq!(65536, tl.feed_and_transform(0));
	}

	#[test]
	fn test_feed_and_transform_wraparound_above_zero() {
		let mut tl = new_tl();
		tl.feed_and_transform(0);
		tl.feed_and_transform(10000);
		tl.feed_and_transform(20000);
		tl.feed_and_transform(30000);
		tl.feed_and_transform(40000);
		tl.feed_and_transform(50000);
		tl.feed_and_transform(60000);
		assert_eq!(65536 + 1200, tl.feed_and_transform(1200));
	}

	#[test]
	fn test_feed_and_transform_slack() {
		let mut tl = new_tl();
		tl.feed_and_transform(0);
		tl.feed_and_transform(10000);
		tl.feed_and_transform(20000);
		tl.feed_and_transform(30000);
		tl.feed_and_transform(40000);
		tl.feed_and_transform(50000);
		tl.feed_and_transform(60000);
		assert_eq!(59001, tl.feed_and_transform(59001));
	}

	#[test]
	fn test_feed_and_transform_slack_after_wraparound() {
		let mut tl = new_tl();
		tl.feed_and_transform(0);
		tl.feed_and_transform(10000);
		tl.feed_and_transform(20000);
		tl.feed_and_transform(30000);
		tl.feed_and_transform(40000);
		tl.feed_and_transform(50000);
		tl.feed_and_transform(60000);
		tl.feed_and_transform(10);
		assert_eq!(65535, tl.feed_and_transform(65535));
	}

	#[test]
	fn test_feed_and_transform_slack_wraparound_slack() {
		let mut tl = new_tl();
		tl.feed_and_transform(0);
		tl.feed_and_transform(10000);
		tl.feed_and_transform(20000);
		tl.feed_and_transform(30000);
		tl.feed_and_transform(40000);
		tl.feed_and_transform(50000);
		tl.feed_and_transform(60000);
		assert_eq!(59001, tl.feed_and_transform(59001));
		tl.feed_and_transform(10);
		assert_eq!(65535, tl.feed_and_transform(65535));
		assert_eq!(65546, tl.feed_and_transform(10));
	}

	#[test]
	fn test_reset_and_feed() {
		let mut tl = new_tl();
		tl.reset(1000);
		assert_eq!(0, tl.feed_and_transform(1000));
		for i in std::iter::successors(Some(1000), |&prev| { let next = prev + 100; if next < 65536 { Some(next) } else { None } }) {
			let i_16 = i as u16;
			assert_eq!(i - 1000, tl.feed_and_transform(i_16));
		}
	}

	#[test]
	fn test_feed_and_transform_slack_after_reset() {
		let mut tl = new_tl();
		assert_eq!(-999, tl.feed_and_transform(64537));
	}

	#[test]
	fn test_forward() {
		let mut tl = new_tl();
		tl.forward(65536 + 5);
		assert_eq!(65536 + 10, tl.feed_and_transform(10));
	}

	#[test]
	fn test_forward_wrapping() {
		let mut tl = new_tl();
		tl.forward(2*65536 + 5);
		assert_eq!(2*65536 + 10, tl.feed_and_transform(10));
	}
}

pub trait RTCifier: Debug {
	fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16);
	fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc>;
	fn reset(&mut self);
	fn ready(&self) -> bool;
}

/// Approximate mapping of a high-precision but low range time counter to a real time clock using infrequent samples of a low-precision RTC timestamp
#[derive(Debug)]
pub struct LinearRTC {
	max_difference: Duration,
	max_history: usize,
	init_history: usize,
	rtcbase: DateTime<Utc>,
	history: VecDeque<(DateTime<Utc>, i64)>,
	timeline: Timeline,
}

impl LinearRTC {
	pub fn new(max_history: usize, max_difference: Duration, init_history: usize, timeline_slack: u16) -> LinearRTC {
		// real limit is i32, but I am too lazy to calculate that
		// limit is because of the Duration div impl
		assert!(max_history < 65535);
		let bogus_date = Utc::now() - Duration::days(10);
		LinearRTC{
			max_difference,
			max_history,
			init_history,
			rtcbase: bogus_date,
			history: VecDeque::new(),
			timeline: Timeline::new(timeline_slack),
		}
	}

	fn truncate_history(&mut self) {
		while self.history.len() >= self.max_history {
			self.history.pop_front();
		}
	}
}

impl Default for LinearRTC {
	fn default() -> Self {
		Self::new(
			1500,
			Duration::seconds(60),
			1,
			30000,
		)
	}
}

impl RTCifier for LinearRTC {
	fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16) {
		self.truncate_history();

		let new_ref = timestamp;
		let offset = self.timeline.feed_and_transform(new_ref);

		for (_, hist_offset) in self.history.iter_mut() {
			*hist_offset = *hist_offset - offset;
		}
		self.history.push_back((rtc, 0));

		if self.history.len() < self.init_history {
			self.history.resize(self.init_history, (rtc, 0));
		}

		let mut deviation_sum = Duration::zero();
		for (hist_rtc, hist_offset) in self.history.iter() {
			deviation_sum = deviation_sum + ((*hist_rtc - Duration::milliseconds(*hist_offset)) - rtc);
		}
		let deviation_avg = deviation_sum / (self.history.len() as i32);

		let new_rtcbase = rtc + deviation_avg;

		self.rtcbase = if deviation_avg >= self.max_difference || deviation_avg <= -self.max_difference {
			self.history.swap_remove_back(0);
			self.history.truncate(1);
			rtc
		} else {
			new_rtcbase
		};

		self.timeline.reset(new_ref);
	}

	fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc> {
		self.rtcbase + Duration::milliseconds(self.timeline.feed_and_transform(timestamp))
	}

	fn reset(&mut self) {
		self.history.clear();
	}

	fn ready(&self) -> bool {
		self.history.len() >= 10
	}
}

#[cfg(test)]
mod tests_rtcifier {
	use super::*;

	use chrono::TimeZone;

	fn new_rtcifier() -> LinearRTC {
		LinearRTC::new(
			1500,
			Duration::seconds(60),
			1,
			1000,
		)
	}

	fn truncated_utcnow() -> DateTime<Utc> {
		let ts = Utc::now();
		ts - Duration::nanoseconds(ts.timestamp_subsec_nanos() as i64)
	}

	#[test]
	fn test_align_maps_directly() {
		let dt0 = truncated_utcnow();
		let t0 = 5;
		let mut rtcifier = new_rtcifier();

		rtcifier.align(dt0, t0);

		assert_eq!(rtcifier.map_to_rtc(5), dt0);
		assert_eq!(rtcifier.map_to_rtc(0), dt0 - Duration::milliseconds(5));
	}

	#[test]
	fn test_align_maps_directly_for_grossly_nonzero_value() {
		let dt0 = truncated_utcnow();
		let t0 = 65000;

		let dt1 = dt0 + Duration::milliseconds(500);
		let t1 = 65500;
		let mut rtcifier = new_rtcifier();

		rtcifier.align(dt0, t0);

		assert_eq!(rtcifier.map_to_rtc(65500), dt0 + Duration::milliseconds(500));
		assert_eq!(rtcifier.map_to_rtc(65200), dt0 + Duration::milliseconds(200));
		assert_eq!(rtcifier.map_to_rtc(64800), dt0 - Duration::milliseconds(200));

		rtcifier.align(dt1, t1);

		assert_eq!(rtcifier.map_to_rtc(65500), dt0 + Duration::milliseconds(500));
		assert_eq!(rtcifier.map_to_rtc(65200), dt0 + Duration::milliseconds(200));
		assert_eq!(rtcifier.map_to_rtc(64800), dt0 - Duration::milliseconds(200));
	}

	#[test]
	fn test_align_learns_over_several_samples() {
		let dt0 = Utc.ymd(2017, 6, 10).and_hms(9, 41, 0);
		let mut rtcifier = new_rtcifier();

		rtcifier.align(dt0, 0);
		assert_eq!(rtcifier.map_to_rtc(0), dt0);

		rtcifier.align(dt0 + Duration::seconds(1), 1000);
		assert_eq!(rtcifier.map_to_rtc(1000), dt0 + Duration::seconds(1));

		rtcifier.align(dt0 + Duration::seconds(3), 2000);
		assert_eq!(rtcifier.map_to_rtc(2000), dt0 + Duration::nanoseconds(2333333334));

		rtcifier.align(dt0 + Duration::seconds(4), 3000);
		assert_eq!(rtcifier.map_to_rtc(3000), dt0 + Duration::milliseconds(3500));
	}

	#[test]
	fn test_align_resets_completely_on_large_difference() {
		let dt0 = Utc.ymd(2017, 6, 10).and_hms(9, 41, 0);
		let mut rtcifier = new_rtcifier();

		rtcifier.align(dt0, 0);
		assert_eq!(rtcifier.map_to_rtc(0), dt0);

		rtcifier.align(dt0 + Duration::seconds(1), 1000);
		assert_eq!(rtcifier.map_to_rtc(1000), dt0 + Duration::seconds(1));

		rtcifier.align(dt0 + Duration::seconds(3), 2000);
		assert_eq!(rtcifier.map_to_rtc(2000), dt0 + Duration::nanoseconds(2333333334));

		rtcifier.align(dt0 + Duration::seconds(120), 3000);
		assert_eq!(rtcifier.map_to_rtc(3000), dt0 + Duration::seconds(120));
	}
}

#[derive(Debug, Clone, Copy)]
struct Range{
	lower: i64,
	upper: i64,
}

impl Range {
	fn shift(&mut self, offset: i64) {
		self.lower -= offset;
		self.upper -= offset;
		if self.lower <= -1000 && self.upper <= -1000 {
			self.lower += 1000;
			self.upper += 1000;
		}
	}

	fn threshold(&self) -> i64 {
		self.upper / 2 + self.lower / 2
	}

	fn reconcile(&mut self, new_lower: i64, new_upper: i64) {
		assert!(self.upper >= self.lower);

		let (curr_lower, curr_upper, map_offset) = if self.lower <= -1000 {
			(self.lower + 1000, self.upper + 1000, -1000i64)
		} else {
			(self.lower, self.upper, 0i64)
		};

		if curr_lower < new_lower {
			self.lower = new_lower + map_offset
		};
		if curr_upper > new_upper {
			self.upper = new_upper + map_offset
		};

		if self.upper < self.lower {
			// oh no. this means that there was no overlap between the two ranges. that means that one of the two ranges was wrong.
			// most likely cause is that one of the involved clocks jittered at some point and we got an incorrect estimate from the upper/lower bound from that
			let range = new_upper / 10 - new_lower / 10 + 1;
			let old_center = curr_lower / 2 + curr_upper / 2;
			self.lower = old_center - range;
			self.upper = old_center + range;
			if self.lower < -1000 {
				self.lower += 1000;
				self.upper += 1000;
			}
			warn!("Range got reconciled into a zero-sized range. clock jitter? {} {} {} {} => {} {}", curr_lower, curr_upper, new_lower, new_upper, self.lower, self.upper);
		}

		self.shift(0);
	}
}

#[derive(Debug)]
struct State{
	rtc: DateTime<Utc>,
}

#[derive(Debug)]
pub struct RangeRTC {
	timeline: Timeline,
	range: Option<Range>,
	state: Option<State>,
}

impl RangeRTC {
	pub fn new(timeline_slack: u16) -> Self {
		Self{
			timeline: Timeline::new(timeline_slack),
			range: None,
			state: None,
		}
	}

	fn get_offset(&self) -> i64 {
		self.range.as_ref().unwrap().threshold()
	}
}

impl Default for RangeRTC {
	fn default() -> Self {
		Self::new(30000)
	}
}

impl RTCifier for RangeRTC {
	fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16) {
		let state = match self.state.as_mut() {
			None => {
				self.state = Some(State{rtc});
				self.timeline.reset(timestamp);
				return;
			},
			Some(st) => st,
		};

		// first, we need to advance the timeline
		let offset = self.timeline.feed_and_transform(timestamp);
		if offset <= 0 {
			// we cannot have that, it'll only cause pain
			return;
		}

		// now we reset it and de-advance all our internal ranges
		self.timeline.reset(timestamp);
		if let Some(r) = self.range.as_mut() {
			r.shift(offset);
		}

		if state.rtc != rtc {
			// yay, new switch point
			let lower_bound = -offset;
			let upper_bound = 0i64;
			// println!("{:?} {} {}", self.range, lower_bound, upper_bound);
			// TODO: somehow aggregate the switch points
			state.rtc = rtc;
			match self.range.as_mut() {
				None => self.range = Some(Range{lower: lower_bound, upper: upper_bound}),
				Some(r) => r.reconcile(lower_bound, upper_bound),
			};
			// println!("{:?} {} {}", self.range, lower_bound, upper_bound);
		}
	}

	fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc> {
		let offset = self.get_offset();
		let timestamp = self.timeline.feed_and_transform(timestamp);
		let ms = timestamp - offset;
		// println!("{} {} {} {}", threshold, timestamp, ms, self.state.as_ref().unwrap().rtc);
		self.state.as_ref().unwrap().rtc + Duration::milliseconds(ms)
	}

	fn reset(&mut self) {
		self.range = None;
	}

	fn ready(&self) -> bool {
		self.range.is_some()
	}
}

#[derive(Debug)]
pub struct RangeRTCv2 {
	timeline: Timeline,
	range: Option<(i64, i64)>,
	state: Option<(DateTime<Utc>, u16, u16)>,
}

impl RangeRTCv2 {
	pub fn new(timeline_slack: u16) -> Self {
		Self{
			timeline: Timeline::new(timeline_slack),
			range: None,
			state: None,
		}
	}

	fn get_offset(&self) -> i64 {
		let (lower, upper) = self.range.unwrap();
		(lower + upper) / 2
	}
}

impl Default for RangeRTCv2 {
	fn default() -> Self {
		Self::new(30000)
	}
}

impl RTCifier for RangeRTCv2 {
	fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16) {
		let state = match self.state.as_mut() {
			None => {
				// this is now our forever epoch for the timestamp value
				self.state = Some((rtc, timestamp, timestamp));
				self.timeline.reset(timestamp);
				return;
			},
			Some(st) => st,
		};

		if state.0 != rtc {
			// we have passed a second, so we now advance all the things
			// first, we need to advance the timeline
			state.2 = state.2.wrapping_add(1000);
			self.timeline.reset(state.2);
			let lower_bound = self.timeline.feed_and_transform(state.1);
			let upper_bound = self.timeline.feed_and_transform(timestamp);

			self.range = Some((lower_bound, upper_bound));
			state.0 = rtc;
		}

		// advance the internal timestamp value so that we know the bound
		// for a transition
		state.1 = timestamp;
	}

	fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc> {
		let offset = self.get_offset();
		let timestamp = self.timeline.feed_and_transform(timestamp);
		let ms = timestamp - offset;
		eprintln!("{} {} {} {:?}", offset, timestamp, ms, self.state);
		self.state.as_ref().unwrap().0 + Duration::milliseconds(ms)
	}

	fn reset(&mut self) {
		self.range = None;
	}

	fn ready(&self) -> bool {
		self.range.is_some()
	}
}

#[derive(Debug)]
pub struct FilteredRTC {
	timeline: Timeline,
	rangertc: RangeRTC,
	offsets: Vec<i64>,
	max_offsets: usize,
	take_from_end: bool,
}

impl FilteredRTC {
	pub fn new(timeline_slack: u16, history: usize) -> Self {
		Self{
			timeline: Timeline::new(timeline_slack),
			rangertc: RangeRTC::new(timeline_slack),
			offsets: Vec::new(),
			max_offsets: history,
			take_from_end: false,
		}
	}
}

impl Default for FilteredRTC {
	fn default() -> Self {
		Self::new(30000, 128)
	}
}

fn ring_median(vs: &[i64]) -> Option<i64> {
	// the trick is that we know that the numbers are mod 1000 (but negative) we also assume that we have cluster of numbers, so that there will be a difference >= 500 between the two ends of the cluster (even under the ring arithmetic).
	//
	// We thus search for the place where the -500 would be. If the two numbers next to that place have an absolute difference less than 500, we assume that the element to the right hand side is the smallest element minus 1000 and the element to the left hand side is the largest one.
	let len = vs.len();
	if len == 0 {
		return None;
	}

	let index = len / 2;
	let (ioffset, voffset) = match vs.binary_search(&-500) {
		// we have a contiguous cluster of numbers, no wraparound
		Ok(_) => (0, 0),
		Err(i) if i == 0 => (0, 0),
		Err(i) if i == len => (0, 0),
		Err(i) => {
			// we now know for sure that the index has a predecessor and a successor, otherwise it'd be equal to zero or len - 1.
			let v1 = vs[i - 1];
			let v2 = vs[i];
			if v2 - v1 >= 500 {
				// non-contiguous case, use the number at the insertion point as new zero, because it is the smallest number
				(i, -1000)
			} else {
				// contiguous
				(0, 0)
			}
		},
	};
	let mapped_index1 = (index + ioffset).rem_euclid(len);

	let v = if len % 2 == 1 {
		let v = vs[mapped_index1];
		if mapped_index1 < ioffset {
			v - voffset
		} else {
			v
		}
	} else {
		let mapped_index0 = (index + ioffset - 1).rem_euclid(len);
		let mut v1 = vs[mapped_index1];
		let mut v2 = vs[mapped_index0];
		if mapped_index0 >= ioffset {
			v2 += voffset;
		}
		if mapped_index1 >= ioffset {
			v1 += voffset;
		}
		(v1 + v2) / 2
	};

	Some(if v >= 1000 {
		v - 1000
	} else if v <= -1000 {
		v + 1000
	} else {
		v
	})
}

#[cfg(test)]
mod test_ring_median {
	use super::ring_median;

	#[test]
	fn trivial() {
		assert!(ring_median(&[][..]).is_none());
		assert_eq!(ring_median(&[0]).unwrap(), 0);
	}

	#[test]
	fn simple_odd_number_of_elements() {
		assert_eq!(ring_median(&[-30, -20, -10][..]).unwrap(), -20);
		assert_eq!(ring_median(&[-30, -25, -20, -15, -10][..]).unwrap(), -20);
	}

	#[test]
	fn simple_even_number_of_elements() {
		assert_eq!(ring_median(&[-30, -20, -20, -10][..]).unwrap(), -20);
		assert_eq!(ring_median(&[-30, -25, -15, -10][..]).unwrap(), -20);
		assert_eq!(ring_median(&[-30, -20, -15, -10][..]).unwrap(), -17);
	}

	#[test]
	fn low() {
		assert_eq!(ring_median(&[-990, -980, -970][..]).unwrap(), -980);
		assert_eq!(ring_median(&[-990, -980, -970, -960][..]).unwrap(), -975);
	}

	#[test]
	fn wraparound_odd_number_of_elements_without_zero() {
		assert_eq!(ring_median(&[-990, -20, -10]).unwrap(), -10);
		assert_eq!(ring_median(&[-990, -980, -20, -15, -10]).unwrap(), -10);
		assert_eq!(ring_median(&[-990, -20, -15, -12, -10]).unwrap(), -12);
	}

	#[test]
	fn wraparound_odd_number_of_elements_with_zero() {
		assert_eq!(ring_median(&[-990, -10, 0]).unwrap(), 0);
		assert_eq!(ring_median(&[-990, -980, -20, -10, 0]).unwrap(), 0);
	}

	#[test]
	fn wraparound_even_number_of_elements() {
		assert_eq!(ring_median(&[-990, -980, -20, -10][..]).unwrap(), 0);
		assert_eq!(ring_median(&[-990, -980, -970, -20][..]).unwrap(), 15);
		assert_eq!(ring_median(&[-990, -20, -10, 0][..]).unwrap(), -5);
	}

	#[test]
	fn testcase1() {
		let mut vs = vec![-905, -905, -919, -918, -931, -931, -944, -944, -957, -957, -970, -970, -983, -983, -996, -996, -9, -9, -22, -22, -35, -35, -48, -48, -61, -61, -74];
		vs.sort();
		assert_eq!(ring_median(&vs).unwrap(), 17);
	}

	#[test]
	fn testcase1_1() {
		let mut vs = vec![-905, -905, -919, -918, -931, -931, -944, -944, -957, -957, -970, -970, -983, -983, -983, -996, -996, -9, -9, -22, -22, -35, -35, -48, -48, -61, -61, -74];
		vs.sort();
		assert_eq!(ring_median(&vs).unwrap(), 17);
	}

	#[test]
	fn testcase2() {
		let mut vs = vec![-983, -983, -997, -996, -9, -9, -22, -22, -35, -35, -48, -48, -61, -61, -74, -74, -87, -87, -100, -100, -113];
		vs.sort();
		assert_eq!(ring_median(&vs).unwrap(), -48);
	}

	#[test]
	fn testcase2_1() {
		let mut vs = vec![-983, -983, -997, -996, -9, -9, -22, -22, -35, -35, -48, -48, -48, -61, -61, -74, -74, -87, -87, -100, -100, -113];
		vs.sort();
		assert_eq!(ring_median(&vs).unwrap(), -48);
	}

	#[test]
	fn testcase3() {
		let mut vs = vec![-936, -936, -949, -949, -962, -962, -975, -975, -988, -988, -1, -1, -1, -991, -992, -991, -992, -991, -992, -992, -992, -991, -992, -991, -992, -991, -992, -992, -992, -991, -991, -991, -992, -992, -992, -991, -991, -991, -992, -991, -992, -992, -991, -992, -991, -991, -992, -991, -992, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -995, -999, -998, -999, -999, -999, -998, -998, -998, -999, -998, -999, -999, -999, -998, -998, -998, -999, -998, -998, -999, -998, -998, -998, -998, -999, -998, -999, -998, -999, -998, -999, -998, -999, -998, -999, -998, -999, -998, -999, -998];
		vs.sort();
		assert_eq!(ring_median(&vs).unwrap(), -995);
	}
}

impl RTCifier for FilteredRTC {
	fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16) {
		let diff = self.timeline.feed_and_transform(timestamp);
		self.rangertc.align(rtc, timestamp);
		if diff <= 0 {
			return;
		}
		self.timeline.reset(timestamp);
		if self.rangertc.ready() {
			for historic in self.offsets.iter_mut() {
				*historic = *historic - diff;
				if *historic <= -1000 {
					*historic += 1000;
				}
			}
			let offset = self.rangertc.get_offset();
			/* if self.offsets.len() >= self.max_offsets {
				if self.take_from_end {
					let src = self.offsets.len() - 1;
					self.offsets[src] = offset;
				} else {
					self.offsets[0] = offset;
				}
				self.take_from_end = !self.take_from_end;
			} else {
				self.offsets.push(offset);
			}
			self.offsets.sort(); */
			// TODO: replace by VecDeque
			if self.offsets.len() >= self.max_offsets {
				self.offsets.remove(0);
			}
			self.offsets.push(offset);
			// eprintln!("{:?}", self.offsets);
		}
	}

	fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc> {
		let mut buf = self.offsets.clone();
		buf.sort();
		let offset = ring_median(&buf).unwrap();
		let timestamp = self.rangertc.timeline.feed_and_transform(timestamp);
		let ms = timestamp - offset;
		// eprintln!("{} {} {} {}", offset, timestamp, ms, self.rangertc.state.as_ref().unwrap().rtc);
		self.rangertc.state.as_ref().unwrap().rtc + Duration::milliseconds(ms)
	}

	fn reset(&mut self) {
		self.rangertc.reset();
		self.offsets.clear();
		self.take_from_end = false;
	}

	fn ready(&self) -> bool {
		self.rangertc.ready() && self.offsets.len() > 0
	}
}


#[cfg(test)]
mod test_rtcifierv2 {
	use super::*;
	use chrono::TimeZone;

	fn new_rtcifier() -> RangeRTC {
		RangeRTC::new(30000)
	}

	#[test]
	fn test_range_shift_operates_mod_1000() {
		let mut r = Range{lower: -10, upper: 0};
		r.shift(500);
		assert_eq!(r.lower, -510);
		assert_eq!(r.upper, -500);
		r.shift(495);
		assert_eq!(r.lower, -1005);
		assert_eq!(r.upper, -995);
		r.shift(5);
		assert_eq!(r.lower, -10);
		assert_eq!(r.upper, 0);
	}

	#[test]
	fn test_reconcile_handles_shrinking_lower_bound() {
		let mut r = Range{lower: -30, upper: -10};
		r.reconcile(-25, 0);
		assert_eq!(r.lower, -25);
		assert_eq!(r.upper, -10);
	}

	#[test]
	fn test_reconcile_handles_shrinking_upper_bound() {
		let mut r = Range{lower: -1010, upper: -990};
		r.reconcile(-25, 0);
		assert_eq!(r.lower, -10);
		assert_eq!(r.upper, 0);
	}

	/* #[test]
	fn test_reconcile_expands_range_around_previous_centerpoint() {
		let mut r = Range{lower: -20, upper: -10};
		let old_center = r.threshold();
		r.reconcile(-5, 0);
		assert_eq!(r.lower, -18);
		assert_eq!(r.upper, -12);
		let new_center = r.threshold();
		assert_eq!(old_center, new_center);
	}

	#[test]
	fn test_reconcile_expands_range_around_previous_centerpoint_with_difficult_offsets() {
		let mut r = Range{lower: -1010, upper: -990};
		let old_center = r.threshold();
		r.reconcile(-1020, -1015);
		assert_eq!(r.lower, -1004);
		assert_eq!(r.upper, -996);
		let new_center = r.threshold();
		assert_eq!(old_center, new_center);
	} */

	#[test]
	fn test_is_unready_after_init() {
		assert!(!new_rtcifier().ready());
	}

	#[test]
	fn test_unready_if_only_equal_rtc_timestamps_are_fed() {
		let mut rtcifier = new_rtcifier();
		let dt0 = Utc.ymd(2021, 8, 1).and_hms(17, 3, 15);
		rtcifier.align(dt0, 1200);
		assert!(!rtcifier.ready());
		rtcifier.align(dt0, 1250);
		assert!(!rtcifier.ready());
		rtcifier.align(dt0, 1270);
		assert!(!rtcifier.ready());
	}

	#[test]
	fn test_uses_midpoint_of_numbers_for_threshold() {
		let mut rtcifier = new_rtcifier();
		let dt0 = Utc.ymd(2021, 8, 1).and_hms(17, 3, 15);
		let dt1 = Utc.ymd(2021, 8, 1).and_hms(17, 3, 16);
		rtcifier.align(dt0, 1200);
		assert!(!rtcifier.ready());
		rtcifier.align(dt0, 1250);
		assert!(!rtcifier.ready());
		rtcifier.align(dt0, 1270);
		assert!(!rtcifier.ready());
		rtcifier.align(dt1, 1300);
		assert!(rtcifier.ready());

		assert_eq!(rtcifier.map_to_rtc(1285), Utc.ymd(2021, 8, 1).and_hms(17, 3, 16));
		assert_eq!(rtcifier.map_to_rtc(1300), Utc.ymd(2021, 8, 1).and_hms_milli(17, 3, 16, 15));
		assert_eq!(rtcifier.map_to_rtc(1400), Utc.ymd(2021, 8, 1).and_hms_milli(17, 3, 16, 115));
		assert_eq!(rtcifier.map_to_rtc(2300), Utc.ymd(2021, 8, 1).and_hms_milli(17, 3, 17, 15));
	}

	#[test]
	fn test_improves_upon_estimate_using_new_samples() {
		let mut rtcifier = new_rtcifier();
		let dt0 = Utc.ymd(2021, 8, 1).and_hms(17, 3, 15);
		let dt1 = Utc.ymd(2021, 8, 1).and_hms(17, 3, 16);
		let dt2 = Utc.ymd(2021, 8, 1).and_hms(17, 3, 17);
		rtcifier.align(dt0, 1270);
		rtcifier.align(dt1, 1300);
		// lower bound is 270, upper bound is 300
		rtcifier.align(dt1, 2200);
		rtcifier.align(dt2, 2290);
		// lower bound is 270, upper bound is 290 -> midpoint is

		assert_eq!(rtcifier.map_to_rtc(2290), Utc.ymd(2021, 8, 1).and_hms_milli(17, 3, 17, 10));
	}
}

#[derive(Debug, Clone)]
struct RangeRTCLioriState {
	rtc_epoch: DateTime<Utc>,
	prev_rtc: DateTime<Utc>,
	prev_abs_ctr: i64,
	rolling_average: Option<i64>,
}

impl RangeRTCLioriState {
	fn new(rtc: DateTime<Utc>, abs_ctr: i64) -> Self {
		Self{
			rtc_epoch: rtc,
			prev_rtc: rtc,
			prev_abs_ctr: abs_ctr,
			rolling_average: None,
		}
	}

	#[allow(dead_code)]
	fn shift(&mut self, offset: i64) {
		match self.rolling_average {
			Some(previous) => {
				self.rolling_average = Some(
					(previous - (previous >> 1)) +
					(offset >> 1)
				);
			},
			None => self.rolling_average = Some(offset),
		}
	}

	#[allow(dead_code)]
	fn get_offset_estimate(&self) -> i64 {
		self.rolling_average.unwrap()
	}
}

#[derive(Debug)]
pub struct RangeRTCLiori {
	timeline: Timeline,
	state: Option<RangeRTCLioriState>,
	min_hist: VecDeque<i64>,
	max_hist: VecDeque<i64>,
}

impl RangeRTCLiori {
	pub fn new(timeline_slack: u16, hist_size: usize) -> Self {
		Self{
			timeline: Timeline::new(timeline_slack),
			state: None,
			min_hist: VecDeque::with_capacity(hist_size),
			max_hist: VecDeque::with_capacity(hist_size),
		}
	}

	fn get_min_estimate(&self) -> i64 {
		*self.min_hist.iter().min().unwrap()
	}

	fn get_max_estimate(&self) -> i64 {
		*self.max_hist.iter().max().unwrap()
	}

	#[allow(dead_code)]
	fn get_raw_offset_estimate(&self) -> i64 {
		let min = self.get_min_estimate();
		let max = self.get_max_estimate();
		(max + min) / 2
	}
}

impl Default for RangeRTCLiori {
	fn default() -> Self {
		Self::new(20000, 128)
	}
}

impl RTCifier for RangeRTCLiori {
	fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16) {
		let state = match self.state.as_mut() {
			None => {
				self.timeline.reset(timestamp);
				self.state = Some(RangeRTCLioriState::new(
					rtc, 0
				));
				return;
			},
			Some(st) => st,
		};

		let abs_ctr = self.timeline.feed_and_transform(timestamp);
		if rtc != state.prev_rtc {
			// second transition
			// calculate the differences
			let prev_rtc_since_epoch = state.prev_rtc - state.rtc_epoch;
			let rtc_since_epoch = rtc - state.rtc_epoch;
			let pre_change_diff_ms = state.prev_abs_ctr - prev_rtc_since_epoch.num_milliseconds();
			let post_change_diff_ms = abs_ctr - rtc_since_epoch.num_milliseconds();
			if self.min_hist.len() >= self.min_hist.capacity() {
				self.min_hist.pop_front();
			}
			if self.max_hist.len() >= self.max_hist.capacity() {
				self.max_hist.pop_front();
			}
			self.min_hist.push_back(post_change_diff_ms);
			self.max_hist.push_back(pre_change_diff_ms);


		}
		state.prev_abs_ctr = abs_ctr;
		state.prev_rtc = rtc;
	}

	fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc> {
		let min = self.get_min_estimate();
		let max = self.get_max_estimate();
		let offset = max / 2 + min / 2;
		let abs_ctr = self.timeline.feed_and_transform(timestamp);
		self.state.as_ref().unwrap().rtc_epoch + Duration::milliseconds(abs_ctr + offset)
	}

	fn reset(&mut self) {
		self.state = None;
		self.min_hist.clear();
		self.max_hist.clear();
	}

	fn ready(&self) -> bool {
		self.min_hist.len() > 0
	}
}
