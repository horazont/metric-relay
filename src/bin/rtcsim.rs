/*!
This is an example which is supposed to simulate some issues around the
recovery of an accurate RTC timestamp from the data transmitted by the
sensor block.
*/
use std::ops::Add;

use env_logger;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand::{Rng, SeedableRng};
use rand_distr::Distribution;

use chrono::{DateTime, Utc, Timelike, Duration, TimeZone};

use metric_relay::sbx::{RTCifier, LinearRTC, RangeRTC, FilteredRTC};


type SimRng = Xoshiro256PlusPlus;

struct State {
	rng: SimRng,
	clock: Duration,
}

impl State {
	fn clock(&self) -> Duration {
		self.clock
	}

	fn rng(&mut self) -> &mut SimRng {
		&mut self.rng
	}
}


trait Imperfection<T> {
	fn mess_with(&mut self, state: &mut State, v: T) -> T;
}


struct DriftError {
	pub rate: f64,
	pub offset: i64,
}

impl DriftError {
	fn get_drift(&self, state: &mut State) -> Duration {
		let clock = (state.clock().num_nanoseconds().unwrap() + self.offset) as f64;
		Duration::nanoseconds((clock * self.rate) as i64)
	}
}

impl Imperfection<DateTime<Utc>> for DriftError {
	fn mess_with(&mut self, state: &mut State, v: DateTime<Utc>) -> DateTime<Utc> {
		v + self.get_drift(state)
	}
}

impl Imperfection<u16> for DriftError {
	fn mess_with(&mut self, state: &mut State, v: u16) -> u16 {
		let drift = self.get_drift(state).num_nanoseconds().unwrap() / 1000000;
		if drift > 0 {
			let drift = drift as u16;
			v.wrapping_add(drift)
		} else {
			let drift = (-drift) as u16;
			v.wrapping_sub(drift)
		}
	}
}


struct JitterError<T: rand_distr::Distribution<f64>> {
	distr: T,
}

impl<T: rand_distr::Distribution<f64>> Imperfection<u16> for JitterError<T> {
	fn mess_with(&mut self, state: &mut State, v: u16) -> u16 {
		let offset_ms = self.distr.sample(state.rng());
		if offset_ms < i16::MIN as f64 {
			v.wrapping_sub(-(i16::MIN as i32) as u16)
		} else if offset_ms > i16::MAX as f64 {
			v.wrapping_add(i16::MAX as u16)
		} else {
			let offset_ms = offset_ms as i16;
			if offset_ms < 0 {
				v.wrapping_sub((-(offset_ms as i32) as u16))
			} else {
				v.wrapping_add(offset_ms as u16)
			}
		}
	}
}


struct AccumJitterError<T: rand_distr::Distribution<f64>> {
	distr: T,
	accum: f64,
}

impl<T: rand_distr::Distribution<f64>> Imperfection<u16> for AccumJitterError<T> {
	fn mess_with(&mut self, state: &mut State, v: u16) -> u16 {
		let jitter = self.distr.sample(state.rng());
		self.accum += jitter;
		let offset_ms = self.accum;
		if offset_ms < i16::MIN as f64 {
			v.wrapping_sub(-(i16::MIN as i32) as u16)
		} else if offset_ms > i16::MAX as f64 {
			v.wrapping_add(i16::MAX as u16)
		} else {
			let offset_ms = offset_ms as i16;
			if offset_ms < 0 {
				v.wrapping_sub((-(offset_ms as i32) as u16))
			} else {
				v.wrapping_add(offset_ms as u16)
			}
		}
	}
}


struct OffsetError<T: Copy> {
	pub offset: T,
}

impl<T: Copy, U: Add<T, Output = U>> Imperfection<U> for OffsetError<T> {
	fn mess_with(&mut self, state: &mut State, v: U) -> U {
		v + self.offset
	}
}


struct Driver {
	ctr_effects: Vec<Box<dyn Imperfection<u16> + 'static>>,
	rtc_effects: Vec<Box<dyn Imperfection<DateTime<Utc>> + 'static>>,
	sample_step: Duration,
	state: State,
	end: Duration,
	epoch: DateTime<Utc>,
}

fn apply<T>(effects: &mut [Box<dyn Imperfection<T> + 'static>], state: &mut State, v: T) -> T {
	let mut result = v;
	for effect in effects.iter_mut() {
		result = effect.mess_with(state, result);
	}
	result
}

impl Driver {
	pub fn new(
			ctr_effects: Vec<Box<dyn Imperfection<u16> + 'static>>,
			rtc_effects: Vec<Box<dyn Imperfection<DateTime<Utc>> + 'static>>,
			rng: SimRng,
			sample_step: Duration,
			nsteps: u16,
			) -> Self
	{
		let end = sample_step * (nsteps as i32);
		Self{
			ctr_effects,
			rtc_effects,
			sample_step,
			state: State{
				clock: Duration::zero(),
				rng,
			},
			end,
			epoch: Utc.ymd(2021, 8, 2).and_hms(15, 41, 0),
		}
	}

	fn get_raw_rtc(&self) -> DateTime<Utc> {
		self.epoch + self.state.clock
	}

	fn get_raw_ctr(&self) -> u16 {
		let ms = self.state.clock.num_milliseconds();
		assert!(ms >= 0);
		ms as u16
	}

	fn get_observed_rtc(&mut self) -> DateTime<Utc> {
		let rtc = self.get_raw_rtc();
		let rtc = apply(&mut self.rtc_effects[..], &mut self.state, rtc);
		rtc.with_nanosecond(0).unwrap()
	}

	fn get_observed_ctr(&mut self) -> u16 {
		let ctr = self.get_raw_ctr();
		apply(&mut self.ctr_effects[..], &mut self.state, ctr)
	}
}

impl Iterator for Driver {
	type Item = (Duration, DateTime<Utc>, DateTime<Utc>, u16);

	fn next(&mut self) -> Option<Self::Item> {
		if self.state.clock >= self.end {
			return None
		}
		let result = (self.state.clock, self.get_raw_rtc(), self.get_observed_rtc(), self.get_observed_ctr());
		eprintln!("CYCLE: raw: {} {}    observed: {} {}", self.get_raw_ctr(), self.get_raw_rtc(), result.3, result.2);
		self.state.clock = (self.state.clock + self.sample_step);
		Some(result)
	}
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
	env_logger::init();

	let mut driver = Driver::new(
		vec![
			// worst case drift caused by the clock jitter
			//Box::new(DriftError{rate: 2.16e-05, offset: 0}),
			// random timer jitter
			// so the millisecond clock of the cpu is driven by its main clock, which is driven by a PLL which is sourced from an 8 MHz oscillator
			// the PLL names a jitter of up to 300ps. it is running at 72 MHz.
			// that is 2% jitter right there.
			// however! we have to take into account that each millisecond tick consists of 72000 main clock ticks, so the jitter somewhat evens out there, in the general case; so it'll be a rather sharp distribution centered around 0, but with long tails in both directions.
			Box::new(AccumJitterError{distr: rand_distr::Normal::new(0.0f64, 0.001f64).unwrap(), accum: 0.0}),
			// delay in the processing queue; this is negative because it makes the timestamp appear earlier than it was in truth
			Box::new(JitterError{distr: rand_distr::Normal::new(-0.2f64, 0.8f64).unwrap()}),
		],
		Vec::new(),
		Xoshiro256PlusPlus::seed_from_u64(8752437625u64),
		Duration::milliseconds(487),
		1000,
	);

	let mut mappers: Vec<Box<dyn RTCifier>> = vec![
		//Box::new(LinearRTC::default()),
		Box::new(RangeRTC::default()),
		Box::new(FilteredRTC::new(20000, 128)),
		// Box::new(FilteredRTC::new(20000, 256)),
		// Box::new(FilteredRTC::new(20000, 512)),
	];
	let mut values: Vec<String> = Vec::new();

	for (clock, real, rtc, ctr) in driver {
		let mut panic = false;
		values.clear();
		for mapper in mappers.iter_mut() {
			mapper.align(rtc, ctr);
			let mapped = if mapper.ready() {
				Some(mapper.map_to_rtc(ctr))
			} else {
				None
			};
			let delta = mapped.and_then(|x| {
				Some((x - real).num_microseconds().unwrap() as f64 / 1e3f64)
			});
			values.push(match delta {
				Some(v) => {
					eprintln!("  {:?}\n    real: {}  mapped: {}  diff: {:?}", mapper, real, mapped.unwrap(), v);
					if v > 500.0 || v < -600.0 {
						panic = true;
					}
					format!("{}", v)
				},
				None => "".into(),
			});
		}
		println!("{} {}",
		   clock.num_microseconds().unwrap() as f64 / 1e6f64,
		   &values[..].join(" "),
		);
		if panic {
			panic!("no!");
		}
	}
	Ok(())
}
