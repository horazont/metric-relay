use std::convert::TryInto;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};

use num_enum::TryFromPrimitive;

use bytes::{Buf, BufMut, Bytes};

use crate::serial::SerialNumber;

pub type ConnectionId = u32;
pub static PROTOCOL_VERSION: u8 = 0x00;

#[repr(u8)]
#[derive(TryFromPrimitive, Copy, Clone, Debug)]
pub(crate) enum RawPacketType {
	EchoRequest = 0x01,
	EchoResponse = 0x02,
	AppRequest = 0x03,
	AppResponse = 0x04,
	DataAck = 0x05,
	Data = 0x06,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct RawCommonHeader {
	pub(crate) version: u8,
	pub(crate) type_: RawPacketType,
	pub(crate) connection_id: ConnectionId,
	pub(crate) min_avail_sn: SerialNumber,
	pub(crate) max_recvd_sn: SerialNumber,
	pub(crate) last_recvd_sn: SerialNumber,
}

impl RawCommonHeader {
	pub(crate) const RAW_LEN: usize = 12;

	pub(crate) fn read<R: Buf>(r: &mut R) -> Result<RawCommonHeader, StdIoError> {
		// size of raw header
		if r.remaining() < 1 {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes left for version",
			));
		}
		let version = r.get_u8();
		if version != PROTOCOL_VERSION {
			return Err(StdIoError::new(
				StdIoErrorKind::InvalidData,
				"unsupported version",
			));
		}

		if r.remaining() < 11 {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes left for common header",
			));
		}

		let type_: RawPacketType = match r.get_u8().try_into() {
			Ok(v) => v,
			Err(e) => return Err(StdIoError::new(StdIoErrorKind::InvalidData, e)),
		};

		let connection_id = r.get_u32_le();
		let min_avail_sn = r.get_u16_le().into();
		let max_recvd_sn = r.get_u16_le().into();
		let last_recvd_sn = r.get_u16_le().into();

		Ok(RawCommonHeader {
			version,
			type_,
			connection_id,
			min_avail_sn,
			max_recvd_sn,
			last_recvd_sn,
		})
	}

	pub(crate) fn write<W: BufMut>(&self, w: &mut W) -> Result<(), StdIoError> {
		if w.remaining_mut() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes left for common header",
			));
		}

		w.put_u8(self.version);
		w.put_u8(self.type_ as u8);
		w.put_u32_le(self.connection_id);
		w.put_u16_le(self.min_avail_sn.into());
		w.put_u16_le(self.max_recvd_sn.into());
		w.put_u16_le(self.last_recvd_sn.into());
		Ok(())
	}
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawDataFrameHeader {
	pub(crate) sn: SerialNumber,
	pub(crate) len: u8,
}

impl RawDataFrameHeader {
	pub(crate) const RAW_LEN: usize = 3;

	pub(crate) fn read<R: Buf>(r: &mut R) -> Result<RawDataFrameHeader, StdIoError> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes left for data frame header",
			));
		}

		let sn = r.get_u16_le().into();
		let len = r.get_u8();
		Ok(RawDataFrameHeader { sn: sn, len: len })
	}

	fn write<W: BufMut>(&self, w: &mut W) -> Result<(), StdIoError> {
		if w.remaining_mut() < Self::RAW_LEN {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes left for data frame header",
			));
		}

		w.put_u16_le(self.sn.into());
		w.put_u8(self.len);
		Ok(())
	}
}

pub type RequestId = u32;

#[allow(dead_code)]
pub struct DataAckEntry {
	first: SerialNumber,
	last: SerialNumber,
}

#[derive(Debug, Clone)]
pub struct DataFrame {
	pub sn: SerialNumber,
	pub payload: Bytes,
}

impl DataFrame {
	// false positive
	#[allow(dead_code)]
	pub(crate) fn write<W: BufMut>(&self, w: &mut W) -> Result<(), StdIoError> {
		let len = self.payload.len();
		if len > 255 {
			return Err(StdIoError::new(
				StdIoErrorKind::InvalidInput,
				"payload too large",
			));
		}
		let hdr = RawDataFrameHeader {
			sn: self.sn,
			len: len as u8,
		};
		if w.remaining_mut() < RawDataFrameHeader::RAW_LEN + len {
			return Err(StdIoError::new(
				StdIoErrorKind::UnexpectedEof,
				"not enough bytes left for data frame",
			));
		}
		hdr.write(w)?;
		let mut buf = self.payload.clone();
		w.put(&mut buf);
		Ok(())
	}

	#[inline(always)]
	pub fn sn(&self) -> SerialNumber {
		self.sn
	}

	#[inline(always)]
	pub fn into_payload(self) -> Bytes {
		self.payload
	}
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AppRequest {
	request_id: RequestId,
	request_type: u8,
	payload: Bytes,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AppResponse {
	request_id: RequestId,
	payload: Bytes,
}
