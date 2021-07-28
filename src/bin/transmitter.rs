use tokio;
use std::net;
use tokio::net::UdpSocket;

use bytes::{Bytes, BufMut};

use metric_relay::snurl;
use core::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let raw_sock = UdpSocket::bind(net::SocketAddr::new("0.0.0.0".parse::<net::IpAddr>().unwrap(), 7202u16)).await?;
	let sock = snurl::Socket::new(raw_sock, net::SocketAddr::new("127.0.0.1".parse::<net::IpAddr>().unwrap(), 7201u16));
	let ep = snurl::Endpoint::new(sock);

	let mut ctr = 0u32;
	let mut backing = [0u8; 4];
	loop {
		let mut buf = &mut backing[..];
		buf.put_u32_le(ctr);
		drop(buf);
		ep.send_data(Bytes::copy_from_slice(&backing[..])).await.unwrap();
		ctr = ctr.wrapping_add(1);
		tokio::time::sleep(Duration::new(1, 0)).await;
	}
}
