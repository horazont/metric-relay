#![allow(non_snake_case)]
use bytes::Buf;

use byteorder::{ByteOrder, BigEndian};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalibrationData {
	T1: u16,
	T2: i16,
	T3: i16,
	P1: u16,
	P2: i16,
	P3: i16,
	P4: i16,
	P5: i16,
	P6: i16,
	P7: i16,
	P8: i16,
	P9: i16,
	H1: u8,
	H2: i16,
	H3: u8,
	H4: i16,
	H5: i16,
	H6: i8,
}

impl CalibrationData {
	pub fn from_registers(mut dig88: &[u8], mut dige1: &[u8]) -> CalibrationData {
		assert!(dig88.len() >= 26);
		assert!(dige1.len() >= 7);
		let dig88buf = &mut dig88;
		let dige1buf = &mut dige1;
		let H2 = dige1buf.get_i16_le();
		let H3 = dige1buf.get_u8();
		let H45_1 = dige1buf.get_u8();
		let H45_2 = dige1buf.get_u8();
		let H45_3 = dige1buf.get_u8();
		let H6 = dige1buf.get_i8();

		let H4 = (((H45_1 as u16) << 4) | ((H45_2 as u16) & 0xf)) as i16;
		let H5 = (((H45_3 as u16) << 4) | ((H45_2 as u16) >> 4) & 0xf) as i16;

		CalibrationData{
			T1: dig88buf.get_u16_le(),
			T2: dig88buf.get_i16_le(),
			T3: dig88buf.get_i16_le(),
			P1: dig88buf.get_u16_le(),
			P2: dig88buf.get_i16_le(),
			P3: dig88buf.get_i16_le(),
			P4: dig88buf.get_i16_le(),
			P5: dig88buf.get_i16_le(),
			P6: dig88buf.get_i16_le(),
			P7: dig88buf.get_i16_le(),
			P8: dig88buf.get_i16_le(),
			P9: dig88buf.get_i16_le(),
			H1: {
				dig88buf.advance(1);
				dig88buf.get_u8()
			},
			H2,
			H3,
			H4,
			H5,
			H6,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readout {
	pub pressure: i32,
	pub temp: i32,
	pub humidity: i32,
}

impl Readout {
	pub fn from_registers(readout: &[u8]) -> Readout {
		assert!(readout.len() >= 8);
		Readout{
			pressure: ((((readout[0] as u32) << 16) | ((readout[1] as u32) << 8) | (readout[2] as u32)) >> 4) as i32,
			temp: ((((readout[3] as u32) << 16) | ((readout[4] as u32) << 8) | (readout[5] as u32)) >> 4) as i32,
			humidity: BigEndian::read_u16(&readout[6..8]) as i32,
		}
	}

	/// Decode a helper value for the temperature
	fn get_temp_fine(&self, c: &CalibrationData) -> i32 {
		let var1: i32 = ((((self.temp >> 3) - ((c.T1 as i32) << 1))) * (c.T2 as i32)) >> 11;
		let var2: i32 = (((((self.temp >> 4) - (c.T1 as i32)) * ((self.temp >> 4) - (c.T1 as i32))) >> 12) * (c.T3 as i32)) >> 14;
		var1 + var2
	}

	/// Decode the temperature in units of 0.01°C
	fn get_temperature(t_fine: i32) -> i32 {
		(t_fine * 5 + 128) >> 8
	}

	fn get_pressure(&self, c: &CalibrationData, t_fine: i32) -> u32 {
		let var1: i64 = ((t_fine as i64)) - 128000;
		let var2: i64 = var1 * var1 * (c.P6 as i64);
		let var2: i64 = var2 + ((var1*(c.P5 as i64))<<17);
		let var2: i64 = var2 + (((c.P4 as i64))<<35);
		let var1: i64 = ((var1 * var1 * (c.P3 as i64))>>8) + ((var1 * (c.P2 as i64))<<12);
		let var1: i64 = (((((1 as i64))<<47)+var1))*((c.P1 as i64))>>33;
		if var1 == 0 {
			return 0;
		}
		let p: i64 = 1048576-(self.pressure as i64);
		let p: i64 = (((p<<31)-var2)*3125)/var1;
		let var1: i64 = (((c.P9 as i64)) * (p>>13) * (p>>13)) >> 25;
		let var2: i64 = (((c.P8 as i64)) * p) >> 19;
		let p: i64 = ((p + var1 + var2) >> 8) + (((c.P7 as i64))<<4);
		p as u32
	}

	fn get_humidity(&self, c: &CalibrationData, t_fine: i32) -> u32 {
		let v: i32 = t_fine - ((76800 as i32));
		let v: i32 = ((((self.humidity << 14) - (((c.H4 as i32)) << 20) - (((c.H5 as i32)) * v)) + ((16384 as i32))) >> 15) * (((((((v * ((c.H6 as i32))) >> 10) * (((v * ((c.H3 as i32))) >> 11) + ((32768 as i32)))) >> 10) + ((2097152 as i32))) * ((c.H2 as i32)) + 8192) >> 14);
		let v: i32 = v - (((((v >> 15) * (v >> 15)) >> 7) * ((c.H1 as i32))) >> 4);
		let v: i32 = if v < 0 { 0 } else { v };
		let v: i32 = if v > 419430400 { 419430400 } else { v };
		(v >> 12) as u32
	}

	/// Decode the raw values using the given calibration data.
	///
	/// Returns a triple:
	/// - temperature as hundreth °C (0.01°C, i.e. 23.42°C correspond to a return value of 2342)
	/// - pressure in fixed-point 24.8 format, Pa unit, i.e. 963.862 hPa = 96386.2 Pa correspond to a return value of 24674867
	/// - humidity in fixed-point 22.10 format, %rH unit, i.e. 46.333%rH correspond to a return value of 47445
	pub fn decode(&self, c: &CalibrationData) -> (i32, u32, u32) {
		let t_fine = self.get_temp_fine(c);
		let T = Self::get_temperature(t_fine);
		let P = self.get_pressure(c, t_fine);
		let H = self.get_humidity(c, t_fine);
		(T, P, H)
	}

	/// Decode the raw values into floating point numbers.
	///
	/// Returns a triple:
	/// - temperature as °C
	/// - pressure as Pa
	/// - humidity as %rH
	pub fn decodef(&self, c: &CalibrationData) -> (f64, f64, f64) {
		let (T, P, H) = self.decode(c);
		(
			(T as f64) / 100.0,
			(P as f64) / 256.0,
			(H as f64) / 1024.0,
		)
	}
}

pub static TEMPERATURE_COMPONENT: &'static str = "temperature";
pub static PRESSURE_COMPONENT: &'static str = "pressure";
pub static HUMIDITY_COMPONENT: &'static str = "humidity";
