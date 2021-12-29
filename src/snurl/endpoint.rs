use super::socket::{RecvItem, Socket};
// use std::io::{Error as StdIoError};
use std::error::Error;

use bytes::Bytes;

use tokio;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Debug, Clone)]
enum Function {
	SendData(Bytes),
}

#[derive(Debug)]
pub struct Endpoint {
	cmdch: Sender<Function>,
	pktch: Receiver<RecvItem>,
}

impl Endpoint {
	pub fn new(s: Socket) -> Endpoint {
		let (cmdch, cmdrx) = channel(8);
		let (pktsink, pktch) = channel(1);
		tokio::spawn(async move { Self::_main_loop(s, cmdrx, pktsink).await.unwrap() });
		Endpoint { cmdch, pktch }
	}

	async fn _main_loop(
		mut s: Socket,
		mut cmdrx: Receiver<Function>,
		pktsink: Sender<RecvItem>,
	) -> Result<(), Box<dyn Error>> {
		loop {
			tokio::select! {
				payload = s.recv_packet() => pktsink.send(payload?).await?,
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

	pub async fn recv_data(&mut self) -> Option<RecvItem> {
		self.pktch.recv().await
	}
}
