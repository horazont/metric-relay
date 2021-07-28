use std::collections::VecDeque;
use chrono::{DateTime, Utc, Duration};

/// Map a local high-range, high-precision counter to a remote low-range counter of the same precision.
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

/// Approximate mapping of a high-precision but low range time counter to a real time clock using infrequent samples of a low-precision RTC timestamp
pub struct RTCifier {
	max_difference: Duration,
	max_history: usize,
	init_history: usize,
	rtcbase: DateTime<Utc>,
	history: VecDeque<(DateTime<Utc>, i64)>,
	timeline: Timeline,
}

impl RTCifier {
	pub fn new(max_history: usize, max_difference: Duration, init_history: usize, timeline_slack: u16) -> RTCifier {
		// real limit is i32, but I am too lazy to calculate that
		// limit is because of the Duration div impl
		assert!(max_history < 65535);
		let bogus_date = Utc::now() - Duration::days(10);
		RTCifier{
			max_difference,
			max_history,
			init_history,
			rtcbase: bogus_date,
			history: VecDeque::new(),
			timeline: Timeline::new(timeline_slack),
		}
	}

	pub fn default() -> Self {
		Self::new(
			1500,
			Duration::seconds(60),
			2,
			30000,
		)
	}

	fn truncate_history(&mut self) {
		while self.history.len() >= self.max_history {
			self.history.pop_front();
		}
	}

	pub fn align(&mut self, rtc: DateTime<Utc>, timestamp: u16) {
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

	pub fn map_to_rtc(&mut self, timestamp: u16) -> DateTime<Utc> {
		self.rtcbase + Duration::milliseconds(self.timeline.feed_and_transform(timestamp))
	}

	pub fn reset(&mut self) {
		self.history.clear();
	}
}

#[cfg(test)]
mod tests_rtcifier {
	use super::*;

	use chrono::TimeZone;

	fn new_rtcifier() -> RTCifier {
		RTCifier::new(
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
