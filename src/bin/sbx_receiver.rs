use tokio;
use std::net;
use tokio::net::UdpSocket;

use bytes::{Buf};

use chrono::{Utc, TimeZone};

use metric_relay::snurl;
use metric_relay::sbx;
use metric_relay::sbx::{ReadoutIterable, RTCifier};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "receiver")]
struct Opt {
	#[structopt(short, long, default_value = "7284")]
	src_port: u16,
	#[structopt(short, long, default_value = "7285")]
	dst_port: u16,
}

fn dump<R: Buf>(rtcifier: &mut sbx::LinearRTC, aligned: &mut bool, b: &mut R) -> std::io::Result<()> {
	let hdr = sbx::EspMessageHeader::read(b)?;
	println!("{:x?}", hdr);
	match hdr.type_ {
		sbx::EspMessageType::Status => {
			let _status = sbx::EspStatus::read(b)?;
			// println!("  {:?}", status);
		},
		sbx::EspMessageType::DataPassthrough => {
			let msg = sbx::Message::read(b)?;
			if let sbx::Message::Status(ref status) = msg {
				if hdr.timestamp != 0 {
					let rtc = Utc.timestamp(hdr.timestamp as i64, 0);
					rtcifier.align(rtc, status.uptime);
					*aligned = true;
				}
			}
			/* if let sbx::Message::ReadoutData(sbx::ReadoutMessage::BME280(ref msg)) = msg {
				println!("  dig88={:?}; dige1={:?}; readout={:?}",
					Bytes::copy_from_slice(&msg.dig88[..]),
					Bytes::copy_from_slice(&msg.dige1[..]),
					Bytes::copy_from_slice(&msg.readout[..]));
			} */
			if *aligned {
				for readout in msg.readouts(rtcifier) {
					println!("  {}", readout.timestamp);
					println!("    {} @ {}", readout.path.device_type, readout.path.instance);
					for (comp, value) in readout.components.iter() {
						println!("      {} = {} {}", comp, value.magnitude, value.unit);
					}
				}
			}
			// println!("  {:?}", msg);
		},
	}
	Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let opt = Opt::from_args();

	let raw_sock = UdpSocket::bind(net::SocketAddr::new("0.0.0.0".parse::<net::IpAddr>().unwrap(), opt.src_port)).await?;
	let sock = snurl::Socket::new(raw_sock, net::SocketAddr::new("255.255.255.255".parse::<net::IpAddr>().unwrap(), opt.dst_port));
	let mut ep = snurl::Endpoint::new(sock);

	let mut rtcifier = sbx::LinearRTC::default();
	let mut aligned = false;

	loop {
		if let Some(mut buf) = ep.recv_data().await {
			/*let mut fullbuf = BytesMut::new();
			fullbuf.put(buf);
			fullbuf.resize(133, 0u8);
			println!("{:?}", unsafe { sbx::TransmutedDump::from_bytes(&fullbuf[5..]) }); */
			match dump(&mut rtcifier, &mut aligned, &mut buf) {
				Err(e) => eprintln!("failed to process packet: {}", e),
				Ok(()) => (),
			}
		} else {
			break
		}
	}

	Ok(())
}
