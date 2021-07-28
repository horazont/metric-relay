use std::convert::TryInto;
use std::io::{Result as StdIoResult, Error as StdIoError, ErrorKind as StdIoErrorKind};
use bytes::{Buf, BufMut, Bytes};

use num_enum::TryFromPrimitive;

#[repr(u8)]
#[derive(TryFromPrimitive, Copy, Clone, Debug)]
pub enum EspMessageType {
	DataPassthrough = 0x00,
	Status = 0x01,
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
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for ESP8266 header"))
		}

		let timestamp = r.get_u32_le();
		let type_: EspMessageType = match r.get_u8().try_into() {
			Ok(v) => v,
			Err(e) => return Err(StdIoError::new(StdIoErrorKind::InvalidData, e)),
		};

		Ok(Self{
			timestamp,
			type_,
		})
	}

	pub fn write<W: BufMut>(&self, w: &mut W) -> StdIoResult<()> {
		if w.remaining_mut() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for ESP8266 header"))
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
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for ESP8266 status report"))
		}

		let tx_sent = r.get_u32_le();
		let tx_dropped = r.get_u32_le();
		let tx_oom_dropped = r.get_u32_le();
		let tx_error = r.get_u32_le();
		let tx_retransmitted = r.get_u32_le();
		let tx_broadcasts = r.get_u32_le();
		let tx_queue_overrun = r.get_u32_le();
		let tx_acklocks_needed = r.get_u32_le();

		Ok(Self{
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


#[repr(u8)]
#[derive(TryFromPrimitive, Copy, Clone, Debug)]
pub enum SbxMessageType {
	Ping = 0x01,
	Hello = 0x80,
	Pong = 0x81,
	Status = 0x82,
	SensorDS18B20 = 0xf1,
	SensorNoise = 0xf2,
	SensorDHT11 = 0xf3,
	SensorLight = 0xf4,
	SensorBME280 = 0xf5,
	SensorStreamAccelX = 0xf8,
	SensorStreamAccelY = 0xf9,
	SensorStreamAccelZ = 0xfa,
	SensorStreamCompassX = 0xfb,
	SensorStreamCompassY = 0xfc,
	SensorStreamCompassZ = 0xfd,
	Reserved = 0xff,
}

impl SbxMessageType {
	pub const RAW_LEN: usize = std::mem::size_of::<u8>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for SBX message type"))
		}
		match r.get_u8().try_into() {
			Ok(v) => Ok(v),
			Err(e) => Err(StdIoError::new(StdIoErrorKind::InvalidData, e)),
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub struct DS18B20Id(pub [u8; 8]);

#[derive(Debug, Clone, Copy)]
pub struct DS18B20Sample {
	pub id: DS18B20Id,
	pub raw_value: i16,
}

impl DS18B20Sample {
	pub const RAW_LEN: usize = std::mem::size_of::<u16>() + 8;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for DS18B20 sample"))
		}

		let mut id = [0u8; 8];
		for v in id.iter_mut() {
			*v = r.get_u8();
		}
		let raw_value = r.get_i16_le();

		Ok(Self{
			id: DS18B20Id(id),
			raw_value,
		})
	}
}

#[derive(Debug, Clone)]
pub struct SbxDS18B20Message {
	pub timestamp: u16,
	pub samples: Vec<DS18B20Sample>,
}

impl SbxDS18B20Message {
	pub const RAW_BASE_LEN: usize = std::mem::size_of::<u16>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_BASE_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for sample timestamp"))
		}

		let timestamp = r.get_u16_le();
		let mut samples = Vec::new();

		loop {
			if r.remaining() == 0 {
				break;
			}
			samples.push(DS18B20Sample::read(r)?);
		}

		Ok(Self{
			timestamp,
			samples,
		})
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct LightSample {
	pub timestamp: u16,
	pub ch: [u16; 4],
}

impl LightSample {
	pub const RAW_LEN: usize = std::mem::size_of::<u16>() * 5;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for light sample"))
		}

		let timestamp = r.get_u16_le();
		let mut ch = [0u16; 4];
		for v in ch.iter_mut() {
			*v = r.get_u16_le();
		}

		Ok(Self{
			timestamp,
			ch,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxLightMessage {
	pub samples: [LightSample; 6],
}

impl SbxLightMessage {
	pub const RAW_LEN: usize = LightSample::RAW_LEN * 6;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for light message"))
		}

		let mut samples = [LightSample::default(); 6];
		for v in samples.iter_mut() {
			*v = LightSample::read(r)?;
		}

		Ok(Self{
			samples,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxIMUStreamState {
	pub sequence_number: u16,
	pub timestamp: u16,
	pub period: u16,
}

impl SbxIMUStreamState {
	pub const RAW_LEN: usize = std::mem::size_of::<u16>() * 3;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for IMU stream state"))
		}

		let sequence_number = r.get_u16_le();
		let timestamp = r.get_u16_le();
		let period = r.get_u16_le();

		Ok(Self{
			sequence_number,
			timestamp,
			period,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxI2CMetrics {
	pub transaction_overruns: u16,
}

impl SbxI2CMetrics {
	pub const RAW_LEN: usize = std::mem::size_of::<u16>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for I2C metrics"))
		}

		let transaction_overruns = r.get_u16_le();

		Ok(Self{
			transaction_overruns,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxBME280Metrics {
	pub configure_status: u8,
	pub timeouts: u16,
}

impl SbxBME280Metrics {
	pub const RAW_LEN: usize = std::mem::size_of::<u16>() + std::mem::size_of::<u8>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for BME280 metrics"))
		}

		let configure_status = r.get_u8();
		let timeouts = r.get_u16_le();

		Ok(Self{
			configure_status,
			timeouts,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxTxBufferMetrics {
	pub most_allocated: u16,
	pub allocated: u16,
	pub ready: u16,
	pub total: u16,
}

impl SbxTxBufferMetrics {
	pub const RAW_LEN: usize = std::mem::size_of::<u16>() * 4;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for SBX TX buffer metrics"))
		}

		let most_allocated = r.get_u16_le();
		let allocated = r.get_u16_le();
		let ready = r.get_u16_le();
		let total = r.get_u16_le();

		Ok(Self{
			most_allocated,
			allocated,
			ready,
			total,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxStatusMessage {
	pub uptime: u16,
	pub imu_streams: [SbxIMUStreamState; 2],
	pub i2c_metrics: [SbxI2CMetrics; 2],
	pub bme280_metrics: [SbxBME280Metrics; 2],
	pub tx_buffer_metrics: SbxTxBufferMetrics,
	pub cpu_samples: [u16; 0x20],
}

impl SbxStatusMessage {
	pub const RAW_LEN: usize =
		std::mem::size_of::<u32>() +
		std::mem::size_of::<u16>() +
		std::mem::size_of::<u8>() +
		std::mem::size_of::<u8>() + // status message version
		SbxIMUStreamState::RAW_LEN * 2 +
		SbxI2CMetrics::RAW_LEN * 2 +
		SbxBME280Metrics::RAW_LEN * 2 +
		SbxTxBufferMetrics::RAW_LEN +
		std::mem::size_of::<u16>() * 0x20;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for SBX status message"))
		}

		// skip the RTC -- it is not set anymore
		r.advance(std::mem::size_of::<u32>());
		let uptime = r.get_u16_le();
		let protocol_version = r.get_u8();

		if protocol_version != 0x01 {
			return Err(StdIoError::new(StdIoErrorKind::InvalidData, "unsupported protocol version"))
		}

		let status_version = r.get_u8();

		if status_version != 0x06 {
			return Err(StdIoError::new(StdIoErrorKind::InvalidData, "unsupported status version"))
		}

		let imu_streams = [
			SbxIMUStreamState::read(r)?,
			SbxIMUStreamState::read(r)?,
		];

		let i2c_metrics = [
			SbxI2CMetrics::read(r)?,
			SbxI2CMetrics::read(r)?,
		];

		let bme280_metrics = [
			SbxBME280Metrics::read(r)?,
			SbxBME280Metrics::read(r)?,
		];

		let tx_buffer_metrics = SbxTxBufferMetrics::read(r)?;

		let mut cpu_samples = [0u16; 0x20];
		for v in cpu_samples.iter_mut() {
			*v = r.get_u16_le();
		}

		Ok(Self{
			uptime,
			imu_streams,
			i2c_metrics,
			bme280_metrics,
			tx_buffer_metrics,
			cpu_samples,
		})
	}
}

#[derive(Debug, Clone, Copy)]
pub struct SbxBME280Message {
	pub timestamp: u16,
	pub instance: u8,
	pub dig88: [u8; 26],
	pub dige1: [u8; 7],
	pub readout: [u8; 8],
}

impl SbxBME280Message {
	pub const RAW_LEN: usize =
		std::mem::size_of::<u16>() +
		std::mem::size_of::<u8>() +
		std::mem::size_of::<u8>() * (26 + 7 + 8);

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for BME280 message"))
		}

		let timestamp = r.get_u16_le();
		let instance = r.get_u8();
		let mut dig88 = [0u8; 26];
		r.copy_to_slice(&mut dig88[..]);

		let mut dige1 = [0u8; 7];
		r.copy_to_slice(&mut dige1[..]);

		let mut readout = [0u8; 8];
		r.copy_to_slice(&mut readout[..]);

		Ok(Self{
			timestamp,
			instance,
			dig88,
			dige1,
			readout,
		})
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SbxNoiseReadout {
	pub timestamp: u16,
	pub sqavg: u32,
	pub min: i16,
	pub max: i16,
}

impl SbxNoiseReadout {
	pub const RAW_LEN: usize =
		std::mem::size_of::<u16>() +
		std::mem::size_of::<u32>() +
		std::mem::size_of::<i16>() +
		std::mem::size_of::<i16>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for noise readout"))
		}

		let timestamp = r.get_u16_le();
		let sqavg = r.get_u32_le();
		let min = r.get_i16_le();
		let max = r.get_i16_le();

		Ok(Self{
			timestamp,
			sqavg,
			min,
			max,
		})
	}

}

#[derive(Debug, Clone, Copy)]
pub struct SbxNoiseMessage {
	pub factor: u8,
	pub samples: [SbxNoiseReadout; 12],
}

impl SbxNoiseMessage {
	pub const RAW_LEN: usize =
		std::mem::size_of::<u8>() +
		SbxNoiseReadout::RAW_LEN * 12;

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for noise message"))
		}

		let factor = r.get_u8();
		let mut samples = [SbxNoiseReadout::default(); 12];
		for v in samples.iter_mut() {
			*v = SbxNoiseReadout::read(r)?;
		}

		Ok(Self{
			factor,
			samples,
		})
	}
}

#[derive(Debug, Clone)]
pub struct SbxStreamMessage {
	pub seq: u16,
	pub avg: u16,
	pub coded: Bytes,
}

impl SbxStreamMessage {
	pub const RAW_BASE_LEN: usize =
		std::mem::size_of::<u16>() +
		std::mem::size_of::<u16>();

	pub fn read<R: Buf>(r: &mut R) -> StdIoResult<Self> {
		if r.remaining() < Self::RAW_BASE_LEN {
			return Err(StdIoError::new(StdIoErrorKind::UnexpectedEof, "not enough bytes for stream message"))
		}

		let seq = r.get_u16_le();
		let avg = r.get_u16_le();
		let coded = r.copy_to_bytes(r.remaining());

		Ok(Self{
			seq,
			avg,
			coded,
		})
	}
}
