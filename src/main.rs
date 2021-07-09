use tokio;
use std::net;
use tokio::net::UdpSocket;

mod snurl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let raw_sock = UdpSocket::bind(net::SocketAddr::new("0.0.0.0".parse::<net::IpAddr>().unwrap(), 7201u16)).await?;
	let mut sock = snurl::Socket::new(raw_sock);
	loop {
		println!("{:?}", sock.recv_packet().await?);
	}
	Ok(())
}
