use bytes::Buf;

use byteorder::{BigEndian, ByteOrder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalibrationData {
	par_t1: u16,
	par_t2: i16,
	par_t3: i8,
	par_h1: i16,
	par_h2: i16,
	par_h3: i8,
	par_h4: i8,
	par_h5: i8,
	par_h6: i8,
	par_h7: i8,
	par_p1: u16,
	par_p2: i16,
	par_p3: i8,
	par_p4: i16,
	par_p5: i16,
	par_p6: i8,
	par_p7: i8,
	par_p8: i16,
	par_p9: i16,
	par_p10: u8,
}

impl CalibrationData {
	pub fn from_registers(par8a: &[u8], pare1: &[u8]) -> Self {
		assert_eq!(par8a.len(), 23);
		assert_eq!(pare1.len(), 10);

		let pare9 = &mut &pare1[8..];
		let par8a = &mut &par8a[..];
		let par_t1 = pare9.get_u16_le();
		let par_t2 = par8a.get_i16_le();
		let par_t3 = par8a.get_i8();

		let par_h1 = (((pare1[1] & 0xf) as u16) | ((pare1[2] as u16) << 4)) as i16;
		let par_h2 = ((((pare1[1] & 0xf0) >> 4) as u16) | ((pare1[0] as u16) << 4)) as i16;
		let pare4 = &mut &pare1[3..];
		let par_h3 = pare4.get_i8();
		let par_h4 = pare4.get_i8();
		let par_h5 = pare4.get_i8();
		let par_h6 = pare4.get_i8();
		let par_h7 = pare4.get_i8();

		// 0x8d is unused
		par8a.get_i8();

		let par_p1 = par8a.get_u16_le();
		let par_p2 = par8a.get_i16_le();
		let par_p3 = par8a.get_i8();

		// 0x93 is unused
		par8a.get_i8();

		let par_p4 = par8a.get_i16_le();
		let par_p5 = par8a.get_i16_le();
		let par_p7 /* sic! */ = par8a.get_i8();
		let par_p6 /* sic! */ = par8a.get_i8();

		// 0x9a is unused
		par8a.get_i8();
		// 0x9b is unused
		par8a.get_i8();

		let par_p8 = par8a.get_i16_le();
		let par_p9 = par8a.get_i16_le();
		let par_p10 = par8a.get_u8();
		assert_eq!(par8a.len(), 0);

		Self {
			par_t1,
			par_t2,
			par_t3,
			par_h1,
			par_h2,
			par_h3,
			par_h4,
			par_h5,
			par_h6,
			par_h7,
			par_p1,
			par_p2,
			par_p3,
			par_p4,
			par_p5,
			par_p6,
			par_p7,
			par_p8,
			par_p9,
			par_p10,
		}
	}
}

pub static TEMPERATURE_COMPONENT: &'static str = "temperature";
pub static PRESSURE_COMPONENT: &'static str = "pressure";
pub static HUMIDITY_COMPONENT: &'static str = "humidity";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Readout {
	pub temp: i32,
	pub humidity: i16,
	pub pressure: u32,
}

fn i24_bytes_to_i32(xlsb: u8, lsb: u8, msb: u8) -> i32 {
	let vu32 = ((xlsb as u32) << 8) | ((lsb as u32) << 16) | ((msb as u32) << 24);
	vu32 as i32
}

fn i16_bytes_to_i16(lsb: u8, msb: u8) -> i16 {
	let vu16 = (lsb as u16) | ((msb as u16) << 8);
	vu16 as i16
}

fn u24_bytes_to_u32(xlsb: u8, lsb: u8, msb: u8) -> u32 {
	((xlsb as u32) << 8) | ((lsb as u32) << 16) | ((msb as u32) << 24)
}

impl Readout {
	pub fn from_registers(readout: &[u8]) -> Readout {
		assert_eq!(readout.len(), 8);
		// temperature is on offset 3..6
		Readout {
			temp: i24_bytes_to_i32(readout[5], readout[4], readout[3]) >> 12,
			humidity: i16_bytes_to_i16(readout[7], readout[6]),
			pressure: u24_bytes_to_u32(readout[2], readout[1], readout[0]) >> 12,
		}
	}

	fn get_temp_fine(&self, c: &CalibrationData) -> f64 {
		let temp = self.temp as f64 - (c.par_t1 as f64) * 16.0;
		let var1 = (temp / 16384.0) * (c.par_t2 as f64);
		let var2 = (temp / 131072.0) * (temp / 131072.0) * (c.par_t3 as f64) * 16.0;
		var1 + var2
	}

	fn get_temperature(t_fine: f64) -> f64 {
		t_fine / 5120.0
	}

	fn get_humidity(&self, c: &CalibrationData, temp_comp: f64) -> f64 {
		let var1 = (self.humidity as f64)
			- (((c.par_h1 as f64) * 16.0) + (((c.par_h3 as f64) / 2.0) * temp_comp));
		let var2 = var1
			* (((c.par_h2 as f64) / 262144.0)
				* (1.0
					+ (((c.par_h4 as f64) / 16384.0) * temp_comp)
					+ (((c.par_h5 as f64) / 1048576.0) * temp_comp * temp_comp)));
		let var3 = (c.par_h6 as f64) / 16384.0;
		let var4 = (c.par_h7 as f64) / 2097152.0;
		var2 + ((var3 + (var4 * temp_comp)) * var2 * var2)
	}

	fn get_pressure(&self, c: &CalibrationData, t_fine: f64) -> f64 {
		let var1 = (t_fine / 2.0) - 64000.0;
		let var2 = var1 * var1 * ((c.par_p6 as f64) / 131072.0);
		let var2 = var2 + (var1 * (c.par_p5 as f64) * 2.0);
		let var2 = (var2 / 4.0) + (c.par_p4 as f64) * 65536.0;
		let var1 =
			((c.par_p3 as f64) * var1 * var1 / 16384.0 + (c.par_p2 as f64) * var1) / 524288.0;
		let var1 = (1.0 + var1 / 32768.0) * (c.par_p1 as f64);
		if var1 == 0.0 {
			return 0.0;
		}

		let calc_pres = 1048576.0 - (self.pressure as f64);
		let calc_pres = ((calc_pres - (var2 / 4096.0)) * 6250.0) / var1;
		let var1 = ((c.par_p9 as f64) * calc_pres * calc_pres) / 2147483648.0;
		let var2 = calc_pres * ((c.par_p8 as f64) / 32768.0);
		let var3 =
			(calc_pres / 256.0) * (calc_pres / 256.0) * (calc_pres / 256.0) * (c.par_p10 as f64)
				/ 131072.0;
		calc_pres + (var1 + var2 + var3 + (c.par_p7 as f64) * 128.0) / 16.0
	}

	/// Decode the raw values into floating point numbers.
	///
	/// Returns a triple:
	/// - temperature as °C
	/// - pressure as Pa
	/// - humidity as %rH
	pub fn decodef(&self, c: &CalibrationData) -> (f64, f64, f64) {
		let t_fine = self.get_temp_fine(c);
		let t = Self::get_temperature(t_fine);
		let h = self.get_humidity(c, t);
		let p = self.get_pressure(c, t_fine);
		(t, p, h)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// those are validated against the reference implementation
	#[test]
	fn temperature_check() {
		let par8a = [
			229, 103, 3, 224, 252, 137, 178, 215, 88, 0, 21, 27, 236, 255, 9, 30, 0, 0, 37, 5, 132,
			239, 30,
		];
		let pare1 = [64, 48, 43, 0, 45, 20, 120, 156, 122, 103];
		let readout = [160, 140, 92, 26, 64, 122, 47, 48, 77, 254];
		let calibration = CalibrationData::from_registers(&par8a[..], &pare1[..]);
		let (t, _, _) = Readout::from_registers(&readout[2..]).decodef(&calibration);
		assert_eq!(t, 24.298633384272943f64);
	}

	#[test]
	fn humidity_check() {
		let par8a = [
			229, 103, 3, 224, 252, 137, 178, 215, 88, 0, 21, 27, 236, 255, 9, 30, 0, 0, 37, 5, 132,
			239, 30,
		];
		let pare1 = [64, 48, 43, 0, 45, 20, 120, 156, 122, 103];
		let readout = [160, 140, 92, 26, 64, 122, 47, 48, 77, 254];
		let calibration = CalibrationData::from_registers(&par8a[..], &pare1[..]);
		let readout = Readout::from_registers(&readout[2..]);
		let (_, _, h) = readout.decodef(&calibration);
		assert_eq!(h, 46.65664811681228f64);
	}

	#[test]
	fn pressure_check() {
		let pare1 = [63, 4, 49, 0, 45, 20, 120, 156, 66, 102];
		let par8a = [
			92, 103, 3, 240, 119, 141, 137, 215, 88, 0, 92, 32, 114, 255, 31, 30, 0, 0, 199, 250,
			195, 243, 30,
		];
		let readout = [160, 0, 81, 164, 144, 120, 173, 80, 86, 174];
		let calibration = CalibrationData::from_registers(&par8a[..], &pare1[..]);
		let readout = Readout::from_registers(&readout[2..]);
		let (_, p, _) = readout.decodef(&calibration);
		assert_eq!(p / 100., 1001.1644751000223f64);
	}
}
