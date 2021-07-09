use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};
use std::convert::TryInto;

use num_enum::TryFromPrimitive;

use bytes::{Bytes, Buf, BufMut};

use super::serial::SerialNumber;

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
	pub(crate) fn read<R: Buf>(r: &mut R) -> Result<RawCommonHeader, StdIoError> {
		// size of raw header
		if r.remaining() < 1 {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes left for version"))
		}
		let version = r.get_u8();
		if version != 0x00 {
			return Err(StdIoError::new(StdIoErrorKind::InvalidData, "unsupported version"));
		}

		if r.remaining() < 11 {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes left for common header"))
		}

		let type_: RawPacketType = match r.get_u8().try_into() {
			Ok(v) => v,
			Err(e) => return Err(StdIoError::new(StdIoErrorKind::InvalidData, e)),
		};

		let connection_id = r.get_u32_le();
		let min_avail_sn = r.get_u16_le().into();
		let max_recvd_sn = r.get_u16_le().into();
		let last_recvd_sn  = r.get_u16_le().into();

		Ok(RawCommonHeader{
			version,
			type_,
			connection_id,
			min_avail_sn,
			max_recvd_sn,
			last_recvd_sn,
		})
	}

	pub(crate) fn write<W: BufMut>(&self, w: &mut W) -> Result<(), StdIoError> {
		if w.remaining_mut() < 12 {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes left for common header"));
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

pub type RequestId = u32;

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
	pub(crate) fn write<W: BufMut>(&self, w: &mut W) -> Result<(), StdIoError> {
		let len = self.payload.len();
		if len > 255 {
			return Err(StdIoError::new(StdIoErrorKind::InvalidInput, "payload too large"));
		}
		if w.remaining_mut() < 3 + len {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes left for data frame header"));
		}
		w.put_u16_le(self.sn.into());
		w.put_u8(len as u8);
		let mut buf = self.payload.clone();
		w.put(&mut buf);
		Ok(())
	}

	#[inline(always)]
	pub fn sn(&self) -> SerialNumber {
		self.sn
	}

	#[inline(always)]
	pub fn payload<'x>(&'x self) -> &'x Bytes {
		&self.payload
	}

	#[inline(always)]
	pub fn into_payload(self) -> Bytes {
		self.payload
	}
}

#[derive(Debug, Clone)]
pub struct AppRequest {
	request_id: RequestId,
	request_type: u8,
	payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct AppResponse {
	request_id: RequestId,
	payload: Bytes,
}

#[derive(Debug, Clone)]
pub enum PacketPayload {
	AppRequest(AppRequest),
	AppResponse(AppResponse),
	Data(Bytes),
	/// Inform about a number of lost data frames
	DataLoss(usize),
}
