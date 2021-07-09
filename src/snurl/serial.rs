use std::cmp::Ordering;
use std::ops::{Add, Sub};

type usn = u16;
type isn = i16;
static threshold: usn = (usn::MAX >> 1) + 1;

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct SerialNumber(usn);

impl Eq for SerialNumber {}

impl From<SerialNumber> for usn {
	fn from(sn: SerialNumber) -> usn {
		sn.0
	}
}

impl From<usn> for SerialNumber {
	fn from(v: usn) -> SerialNumber {
		SerialNumber(v)
	}
}

impl PartialOrd for SerialNumber {
	fn partial_cmp(&self, other: &SerialNumber) -> Option<Ordering> {
		if self.0 == other.0 {
			return Some(Ordering::Equal)
		}
		let diff = other.0.wrapping_sub(self.0);
		match diff.partial_cmp(&threshold) {
			// equality to the threshold means that we cannot say if the number is less or greater -> return None
			Some(Ordering::Equal) => None,
			// for the other values, it just works
			other => other,
		}
	}
}

fn wrapping_sub_to_signed(v1: usn, v2: usn) -> isn {
	if v1 >= v2 {
		v1.wrapping_sub(v2) as isn
	} else {
		((v2 ^ 0xffff).wrapping_add(1)).wrapping_add(v1) as isn
	}
}

impl Add<usn> for SerialNumber {
	type Output = SerialNumber;

	fn add(self, rhs: usn) -> Self::Output {
		debug_assert!(rhs < threshold);
		self.0.wrapping_add(rhs).into()
	}
}

impl Sub<usn> for SerialNumber {
	type Output = SerialNumber;

	fn sub(self, rhs: usn) -> Self::Output {
		debug_assert!(rhs < threshold);
		self.0.wrapping_sub(rhs).into()
	}
}

impl Sub<SerialNumber> for SerialNumber {
	type Output = isn;

	fn sub(self, rhs: SerialNumber) -> Self::Output {
		self.checked_sub(rhs).expect("attempt to subtract two serial numbers which have an undefined difference")
	}
}

impl SerialNumber {
	pub fn checked_add_u16(self, rhs: usn) -> Option<SerialNumber> {
		if rhs >= threshold {
			return None
		}
		Some(self + rhs)
	}

	pub fn checked_sub_u16(self, rhs: usn) -> Option<SerialNumber> {
		if rhs >= threshold {
			return None
		}
		Some(self - rhs)
	}

	pub fn checked_sub(self, rhs: SerialNumber) -> Option<i16> {
		match self.partial_cmp(&rhs) {
			Some(Ordering::Equal) => Some(0),
			Some(Ordering::Greater) => {
				Some(wrapping_sub_to_signed(self.0, rhs.0))
			},
			Some(Ordering::Less) => {
				Some(-wrapping_sub_to_signed(rhs.0, self.0))
			},
			None => None,
		}
	}
}

#[cfg(test)]
mod tests_SerialNumber {
	use super::*;

	#[test]
	fn test_equal_value_is_equal() {
		let v1: SerialNumber = 0u16.into();
		let v2: SerialNumber = 0u16.into();
		let v3: SerialNumber = 2342u16.into();
		let v4: SerialNumber = 2342u16.into();
		assert_eq!(v1, v2);
		assert_eq!(v2, v1);
		assert_eq!(v3, v4);
		assert_eq!(v4, v3);
	}

	#[test]
	fn test_unequal_value_is_unequal() {
		let v1: SerialNumber = 0u16.into();
		let v2: SerialNumber = 0u16.into();
		let v3: SerialNumber = 2342u16.into();
		let v4: SerialNumber = 2342u16.into();
		assert_ne!(v1, v3);
		assert_ne!(v3, v1);
		assert_ne!(v2, v4);
		assert_ne!(v4, v2);
	}

	#[test]
	fn test_lt_trivial() {
		let v1: SerialNumber = 0u16.into();
		let v2: SerialNumber = 10u16.into();
		let v3: SerialNumber = 20u16.into();
		assert!(v1 < v2);
		assert!(v2 < v3);
		assert!(v1 < v3);
	}

	#[test]
	fn test_gt_trivial() {
		let v1: SerialNumber = 0u16.into();
		let v2: SerialNumber = 10u16.into();
		let v3: SerialNumber = 20u16.into();
		assert!(v2 > v1);
		assert!(v3 > v2);
		assert!(v3 > v1);
	}

	#[test]
	fn test_lt_across_wraparound() {
		let v1: SerialNumber = 65000u16.into();
		let v2: SerialNumber = 65500u16.into();
		let v3: SerialNumber = 0u16.into();
		let v4: SerialNumber = 20000u16.into();
		assert!(v1 < v2);
		assert!(v2 < v3);
		assert!(v1 < v3);
		assert!(v1 < v4);
	}

	#[test]
	fn test_le_across_wraparound() {
		let v1: SerialNumber = 65000u16.into();
		let v2: SerialNumber = 65500u16.into();
		let v3: SerialNumber = 0u16.into();
		let v4: SerialNumber = 20000u16.into();
		assert!(v1 <= v2);
		assert!(v2 <= v3);
		assert!(v1 <= v3);
		assert!(v1 <= v4);
	}

	#[test]
	fn test_gt_across_wraparound() {
		let v1: SerialNumber = 20000u16.into();
		let v2: SerialNumber = 0u16.into();
		let v3: SerialNumber = 65500u16.into();
		let v4: SerialNumber = 65000u16.into();
		assert!(v1 > v2);
		assert!(v2 > v3);
		assert!(v1 > v3);
		assert!(v1 > v4);
	}

	#[test]
	fn test_ge_across_wraparound() {
		let v1: SerialNumber = 20000u16.into();
		let v2: SerialNumber = 0u16.into();
		let v3: SerialNumber = 65500u16.into();
		let v4: SerialNumber = 65000u16.into();
		assert!(v1 >= v2);
		assert!(v2 >= v3);
		assert!(v1 >= v3);
		assert!(v1 >= v4);
	}

	#[test]
	fn test_gt_undefined_on_threshold() {
		let v1: SerialNumber = 200u16.into();
		let v2: SerialNumber = 32968u16.into();
		assert!(!(v1 > v2));
		assert!(!(v2 > v1));
		assert!(!(v1 >= v2));
		assert!(!(v2 >= v1));
	}

	#[test]
	fn test_lt_undefined_on_threshold() {
		let v1: SerialNumber = 200u16.into();
		let v2: SerialNumber = 32968u16.into();
		assert!(!(v1 < v2));
		assert!(!(v2 < v1));
		assert!(!(v1 <= v2));
		assert!(!(v2 <= v1));
	}

	#[test]
	fn test_ne_defined_on_threshold() {
		let v1: SerialNumber = 200u16.into();
		let v2: SerialNumber = 32968u16.into();
		assert_ne!(v1, v2);
		assert_ne!(v2, v1);
	}

	#[test]
	fn test_checked_add_behaves_correctly_on_serial_edge() {
		let v1: SerialNumber = 65500u16.into();
		let v2 = v1.checked_add_u16(1000u16).unwrap();
		let v3: SerialNumber = 964u16.into();
		assert!(v2 > v1);
		assert!(v1 < v2);
		assert_ne!(v1, v2);
		assert_eq!(v2, v3);
	}

	#[test]
	fn test_checked_sub_u16_behaves_correctly_on_serial_edge() {
		let v1: SerialNumber = 964u16.into();
		let v2 = v1.checked_sub_u16(1000u16).unwrap();
		let v3: SerialNumber = 65500u16.into();
		assert!(v1 > v2);
		assert!(v2 < v1);
		assert_ne!(v1, v2);
		assert_eq!(v2, v3);
	}

	#[test]
	fn test_checked_sub_trivial() {
		let v1: SerialNumber = 10u16.into();
		let v2: SerialNumber = 20u16.into();
		assert_eq!(v1.checked_sub(v1).unwrap(), 0i16);
		assert_eq!(v2.checked_sub(v1).unwrap(), 10i16);
		assert_eq!(v1.checked_sub(v2).unwrap(), -10i16);
	}

	#[test]
	fn test_checked_sub_rejects_difference_at_edge() {
		let v1: SerialNumber = 200u16.into();
		let v2: SerialNumber = 32968u16.into();
		match v1.checked_sub(v2) {
			None => (),
			other => panic!("unexpected checked_sub result: {:?}", other),
		}
		match v2.checked_sub(v1) {
			None => (),
			other => panic!("unexpected checked_sub result: {:?}", other),
		}
	}

	#[test]
	fn test_checked_sub_across_edge() {
		let v1: SerialNumber = 65500u16.into();
		let v2: SerialNumber = 10u16.into();
		assert_eq!(v1.checked_sub(v1).unwrap(), 0i16);
		assert_eq!(v1.checked_sub(v2).unwrap(), -46i16);
		assert_eq!(v2.checked_sub(v1).unwrap(), 46i16);
	}
}
