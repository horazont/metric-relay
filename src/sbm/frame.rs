use bytes::{Buf, BufMut};
use std::convert::TryInto;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind, Result as StdIoResult};

use num_enum::TryFromPrimitive;

#[repr(u8)]
#[derive(TryFromPrimitive, Copy, Clone, Debug)]
pub enum EspMessageType {
	DataPassthrough = 0x00,
	Status = 0x01,
	Bme68x = 0x02,
}

#[derive(Debug, Copy, Clone)]
pub struct EspMessageHeader {
	pub timestamp: u32,
	pub type_: EspMessageType,
}

impl EspMessageHeader {
	pub const RAW_LEN: usize = std::mem::size_of::<u32>() + std::mem::size_of::<u8>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes for ESP8266 header",
			));
		}

		let timestamp = r.get_u32_le();
		let type_: EspMessageType = match r.get_u8().try_into() {
			Ok(v) => v,
			Err(e) => return Err(StdIoError::new(StdIoErrorKind::InvalidData, e)),
		};

		Ok(Self { timestamp, type_ })
	}

	pub fn write<W: BufMut>(&self, w: &mut W) -> StdIoResult<()> {
		if w.remaining_mut() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes for ESP8266 header",
			));
		}

		w.put_u32_le(self.timestamp);
		w.put_u8(self.type_ as u8);
		Ok(())
	}
}

#[derive(Debug, Copy, Clone)]
pub struct EspStatus {
	pub tx_sent: u32,
	pub tx_dropped: u32,
	pub tx_oom_dropped: u32,
	pub tx_error: u32,
	pub tx_retransmitted: u32,
	pub tx_broadcasts: u32,
	pub tx_queue_overrun: u32,
	pub tx_acklocks_needed: u32,
}

impl EspStatus {
	pub const RAW_LEN: usize = std::mem::size_of::<u32>() * 8;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes for ESP8266 status report",
			));
		}

		let tx_sent = r.get_u32_le();
		let tx_dropped = r.get_u32_le();
		let tx_oom_dropped = r.get_u32_le();
		let tx_error = r.get_u32_le();
		let tx_retransmitted = r.get_u32_le();
		let tx_broadcasts = r.get_u32_le();
		let tx_queue_overrun = r.get_u32_le();
		let tx_acklocks_needed = r.get_u32_le();

		Ok(Self {
			tx_sent,
			tx_dropped,
			tx_oom_dropped,
			tx_error,
			tx_retransmitted,
			tx_broadcasts,
			tx_queue_overrun,
			tx_acklocks_needed,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct EspBme68xMessage {
	pub instance: u8,
	pub par8a: [u8; 23],
	pub pare1: [u8; 10],
	pub readout: [u8; 10],
}

impl EspBme68xMessage {
	pub const RAW_LEN: usize =
		std::mem::size_of::<u8>() + std::mem::size_of::<u8>() * (23 + 10 + 10);

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes for BME68x message",
			));
		}

		let instance = r.get_u8();
		let mut par8a = [0u8; 23];
		r.copy_to_slice(&mut par8a[..]);

		let mut pare1 = [0u8; 10];
		r.copy_to_slice(&mut pare1[..]);

		let mut readout = [0u8; 10];
		r.copy_to_slice(&mut readout[..]);

		Ok(Self {
			instance,
			par8a,
			pare1,
			readout,
		})
	}
}
