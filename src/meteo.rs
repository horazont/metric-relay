static HI_C1: f64 = -8.784695;
static HI_C2: f64 = 1.61139411;
static HI_C3: f64 = 2.338549f64;
static HI_C4: f64 = -0.14611605;
static HI_C5: f64 = -1.2308094e-2;
static HI_C6: f64 = -1.6424828e-2;
static HI_C7: f64 = 2.211732e-3;
static HI_C8: f64 = 7.2546e-4;
static HI_C9: f64 = -3.582e-6;

static PRESSURE_A: f64 = 0.0065;
static PRESSURE_C: f64 = 0.12;
static PRESSURE_R_STAR: f64 = 287.05;

static KELVIN_OFFSET: f64 = 273.15;

static DP_K2: f64 = 17.62;
static DP_K3: f64 = 243.12;

/// Calculate the heat index from a given temperature and humidity.
///
/// Returns None if the values are out of bounds for a heat index calculation.
pub fn heat_index(temperature: f64, humidity: f64) -> Option<f64> {
	if temperature < 20f64 {
		return None;
	}
	Some(
		HI_C1
			+ HI_C2 * temperature
			+ HI_C3 * humidity
			+ HI_C4 * temperature * humidity
			+ HI_C5 * temperature * temperature
			+ HI_C6 * humidity * humidity
			+ HI_C7 * temperature * temperature * humidity
			+ HI_C8 * humidity * humidity * temperature
			+ HI_C9 * humidity * humidity * temperature * temperature,
	)
}

/// Correct the measured pressure value for the height of the location of
/// measurement.
///
/// This calculates the "pressure at normal zero" from a measured pressure,
/// given additional environmental values.
///
/// The pressure is given (and returned) in units of hPa. Temperature in units
/// of celsius, humidity in %rH. The acceleration of gravity must be given as
/// meter per second square and the height in meters.
pub fn barometric_correction(
	pressure: f64,
	temperature: f64,
	humidity: f64,
	g_0: f64,
	height: f64,
) -> f64 {
	let abs_temperature = temperature + KELVIN_OFFSET;
	let temp_coeff = 6.112 * (DP_K2 * temperature / (DP_K3 + temperature)).exp();
	let humidity_norm = humidity / 100.0;
	pressure
		* (g_0
			/ (PRESSURE_R_STAR
				* (abs_temperature
					+ PRESSURE_C * temp_coeff * humidity_norm
					+ PRESSURE_A * height / 2.0))
			* height)
			.exp()
}

/// Calculate the dewpoint from temperature and humidity.
///
/// Temperature is taken and returned as degree Celsius, the humidity is taken
/// as %rH.
pub fn dewpoint(temperature: f64, humidity: f64) -> f64 {
	if humidity <= 0.0 {
		return -KELVIN_OFFSET;
	}

	let humidity = humidity / 100.0;
	let ln_h = humidity.ln();

	DP_K3 * (DP_K2 * temperature / (DP_K3 + temperature) + ln_h)
		/ (DP_K2 * DP_K3 / (DP_K3 + temperature) - ln_h)
}

pub fn wet_bulb_temperature(temperature: f64, humidity: f64) -> f64 {
	temperature * (0.151977 * (humidity + 8.313659).sqrt()).atan() + (temperature + humidity).atan()
		- (humidity - 1.676331).atan()
		+ 0.00391838 * (humidity).powf(1.5) * (0.023101 * humidity).atan()
		- 4.686035
}

#[cfg(test)]
mod test_barometric_correction {
	use super::*;

	#[test]
	fn reference_test() {
		assert_eq!(
			barometric_correction(1005.0, 23.0, 60.0, 9.81, 135.0,),
			1020.6484499141941f64,
		);
	}
}

#[cfg(test)]
mod test_dewpoint {
	use super::*;

	#[test]
	fn reference_test() {
		assert_eq!(dewpoint(23.42, 42.23), 9.851421915753248,);
	}
}

#[cfg(test)]
mod test_wet_bulb_temperature {
	use super::*;

	#[test]
	fn reference_test() {
		assert_eq!(wet_bulb_temperature(23.42, 42.23), 15.454027588538501,);
	}
}
