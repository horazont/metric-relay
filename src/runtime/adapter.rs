use log::{warn, debug};

use tokio::sync::mpsc;
use tokio::sync::broadcast;

pub struct Serializer<T: 'static + Clone + Send> {
	sink: mpsc::Sender<T>,
}

impl<T: 'static + Clone + Send> Serializer<T> {
	pub fn new(depth: usize) -> (Self, mpsc::Receiver<T>) {
		let (sender, receiver) = mpsc::channel(depth);
		(Self{sink: sender}, receiver)
	}

	pub fn attach(&self, mut src: broadcast::Receiver<T>) {
		let sink = self.sink.clone();
		tokio::spawn(async move {
			loop {
				let item = match src.recv().await {
					// sending side closed, disconnect
					Err(broadcast::error::RecvError::Closed) => {
						debug!("serializer stream exiting because source got closed");
						return
					},
					Err(broadcast::error::RecvError::Lagged(nlost)) => {
						warn!("serializer was too slow; lost {} items", nlost);
						continue
					},
					Ok(item) => item,
				};
				match sink.send(item).await {
					Ok(()) => (),
					// receiving side closed, disconnect
					Err(_) => {
						debug!("serializer stream exiting because destination got closed");
						return
					},
				}
			}
		});
	}
}
