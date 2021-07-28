use super::socket::Socket;
use super::frame::PacketPayload;
// use std::io::{Error as StdIoError};
use std::error::Error;

use bytes::Bytes;

use tokio;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::SendError;

#[derive(Debug, Clone)]
enum Function {
	SendData(Bytes),
}

#[derive(Debug)]
pub struct Endpoint {
	cmdch: Sender<Function>,
	pktch: Receiver<Bytes>,
}

impl Endpoint {
	pub fn new(s: Socket) -> Endpoint {
		let (cmdch, cmdrx) = channel(8);
		let (pktsink, pktch) = channel(1);
		tokio::spawn(async move {
			Self::_main_loop(s, cmdrx, pktsink).await.unwrap()
		});
		Endpoint{
			cmdch,
			pktch,
		}
	}

	async fn _main_loop(mut s: Socket, mut cmdrx: Receiver<Function>, pktsink: Sender<Bytes>) -> Result<(), Box<dyn Error>> {
		loop {
			tokio::select! {
				payload = s.recv_packet() => match payload? {
					PacketPayload::Data(b) => {
						pktsink.send(b).await?;
					},
					other => panic!("unhandled recv: {:?}", other)
				},
				func = cmdrx.recv() => match func {
					Some(Function::SendData(payload)) => {
						s.send_data(payload).await?;
					},
					None => return Ok(()),
				},
			}
		}
	}

	pub async fn send_data<T: Into<Bytes>>(&self, payload: T) -> Result<(), SendError<Bytes>> {
		match self.cmdch.send(Function::SendData(payload.into())).await {
			Ok(_) => Ok(()),
			Err(SendError(Function::SendData(payload))) => Err(SendError(payload)),
		}
	}

	pub async fn recv_data(&mut self) -> Option<Bytes> {
		self.pktch.recv().await
	}
}
