use std::net;
use tokio;
use tokio::net::UdpSocket;

use metric_relay::snurl;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "receiver")]
struct Opt {
	#[structopt(short, long, default_value = "7201")]
	src_port: u16,
	#[structopt(short, long, default_value = "7202")]
	dst_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let opt = Opt::from_args();

	let raw_sock = UdpSocket::bind(net::SocketAddr::new(
		"0.0.0.0".parse::<net::IpAddr>().unwrap(),
		opt.src_port,
	))
	.await?;
	let sock = snurl::Socket::new(
		raw_sock,
		net::SocketAddr::new(
			"255.255.255.255".parse::<net::IpAddr>().unwrap(),
			opt.dst_port,
		),
		false,
	);
	let mut ep = snurl::Endpoint::new(sock);

	loop {
		println!("{:?}", ep.recv_data().await);
	}
}
