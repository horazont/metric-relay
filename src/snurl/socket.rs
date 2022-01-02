use std::fmt;
use std::io::{Error as StdIoError, ErrorKind as StdIoErrorKind};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use tokio::io::Result as IoResult;
use tokio::net::{ToSocketAddrs, UdpSocket};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use getrandom;

use super::frame::{
	AppRequest, AppResponse, ConnectionId, RawCommonHeader, RawDataFrameHeader, RawPacketType,
	PROTOCOL_VERSION,
};
use super::recvqueue::RecvQueue;
use super::sendqueue::SendQueue;
use crate::serial::SerialNumber;

#[derive(Debug, Clone)]
pub enum RecvItem {
	ResyncMarker,
	Data(Bytes),
}

fn get_random_u16() -> u16 {
	let mut backing = [0u8; 2];
	getrandom::getrandom(&mut backing[..]).expect("random data read failed");
	let buf = &mut &backing[..];
	buf.get_u16_le()
}

fn get_random_u32() -> u32 {
	let mut backing = [0u8; 4];
	getrandom::getrandom(&mut backing[..]).expect("random data read failed");
	let buf = &mut &backing[..];
	buf.get_u32_le()
}

#[derive(Debug, Clone)]
enum PacketResult {
	/// The packet lead to no change in state.
	Nop,
	/// Indicates that a packet may be available for returning from the queue
	/// and that an ACK should be sent.
	PacketReceived,
	/// Indicates that an echo request was received and a response should be
	/// sent
	#[allow(dead_code)]
	EchoRequest,
	/// Indicates that an echo response was received
	#[allow(dead_code)]
	EchoResponse,
	/// Application request was received
	#[allow(dead_code)]
	AppRequest(AppRequest),
	/// Application response was received
	#[allow(dead_code)]
	AppResponse(AppResponse),
}

#[derive(Debug, Clone, Copy)]
enum HandshakeResult {
	Synchronized,
	Resynchronized { lowest_sn: SerialNumber },
	PacketsRequired,
}

#[derive(Debug)]
struct SharedState {
	pub last_recvd_sn: AtomicU16,
	pub max_recvd_sn: AtomicU16,
	pub min_avail_sn: AtomicU16,
	// This is a channel from the receiver to the transmitter which is read when a new packet is being enqueued or during tx operations to update the tx window
	pub remote_max_recvd_sn: AtomicU16,
	// Same as above, but obviously inherently more racey. As last_recvd_sn is best-effort anyway, its good enough.
	pub remote_last_recvd_sn: AtomicU16,
	peer_address: RwLock<SocketAddr>,
	connection_id: AtomicU32,
}

impl SharedState {
	fn new(initial_peer_addr: SocketAddr) -> SharedState {
		SharedState {
			last_recvd_sn: AtomicU16::new(65535u16),
			max_recvd_sn: AtomicU16::new(65535u16),
			min_avail_sn: AtomicU16::new(0u16),
			connection_id: AtomicU32::new(0u32),
			remote_max_recvd_sn: AtomicU16::new(0u16),
			remote_last_recvd_sn: AtomicU16::new(0u16),
			peer_address: RwLock::new(initial_peer_addr),
		}
	}

	fn generate_connection_id(&self) -> ConnectionId {
		// TODO: use a real random number
		let new_conn_id = get_random_u32();
		// now we need to atomically set the connection ID to non-zero.
		// otherwise, we could end up in the situation that we emit a packet
		// with one ID and then a packet with another ID during handshaking,
		// breaking convergence.
		match self.connection_id.compare_exchange(
			0,
			new_conn_id,
			Ordering::AcqRel,
			Ordering::Acquire,
		) {
			// we won the race -> return our value
			Ok(_) => new_conn_id,
			// another thread won the race -> return their ID
			Err(other) => other,
		}
	}

	#[inline]
	fn connection_id(&self) -> ConnectionId {
		self.connection_id.load(Ordering::Acquire)
	}

	#[inline]
	fn change_connection_id(&self, old_conn_id: ConnectionId, new_conn_id: ConnectionId) -> bool {
		self.connection_id
			.compare_exchange(
				old_conn_id,
				new_conn_id,
				Ordering::AcqRel,
				Ordering::Acquire,
			)
			.is_ok()
	}

	fn compose_header(&self, type_: RawPacketType) -> RawCommonHeader {
		let min_avail_sn = self.min_avail_sn.load(Ordering::Acquire).into();
		let last_recvd_sn = self.last_recvd_sn.load(Ordering::Acquire).into();
		let max_recvd_sn = self.max_recvd_sn.load(Ordering::Acquire).into();
		RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_,
			connection_id: self.connection_id.load(Ordering::Acquire),
			min_avail_sn,
			max_recvd_sn,
			last_recvd_sn,
		}
	}

	fn set_rx_sns(&self, max_recvd_sn: SerialNumber, last_recvd_sn: SerialNumber) {
		self.max_recvd_sn
			.store(max_recvd_sn.into(), Ordering::Release);
		self.last_recvd_sn
			.store(last_recvd_sn.into(), Ordering::Release);
	}

	fn set_remote_rx_sns(&self, max_recvd_sn: SerialNumber, last_recvd_sn: SerialNumber) {
		self.remote_max_recvd_sn
			.store(max_recvd_sn.into(), Ordering::Release);
		self.remote_last_recvd_sn
			.store(last_recvd_sn.into(), Ordering::Release);
	}

	fn set_tx_sn(&self, min_avail_sn: SerialNumber) {
		self.min_avail_sn
			.store(min_avail_sn.into(), Ordering::Release);
	}

	fn remote_rx_sns(&self) -> (SerialNumber, SerialNumber) {
		let last_recvd_sn = self.remote_last_recvd_sn.load(Ordering::Acquire).into();
		let max_recvd_sn = self.remote_max_recvd_sn.load(Ordering::Acquire).into();
		(max_recvd_sn, last_recvd_sn)
	}

	#[allow(dead_code)]
	fn update_peer_address(&self, addr: SocketAddr) {
		{
			let guard = self.peer_address.read().unwrap();
			if *guard == addr {
				return;
			}
		}
		{
			let mut guard = self.peer_address.write().unwrap();
			*guard = addr;
		}
	}

	fn peer_address(&self) -> SocketAddr {
		self.peer_address.read().unwrap().clone()
	}
}

struct Receiver {
	state: Arc<SharedState>,
	buffer: Option<Box<[u8; 65536]>>,
	q: RecvQueue,
	// in half-synced state, we expect a packet from the remote matching our connection ID to learn their min_avail_sn
	// this state is only relevant for the receiver, so it is safe to keep outside the SharedState. it will be protected by the borrowchecker
	half_synced: bool,
	// packets which can be read right away in this order, typically flushed out of the recvqueue on a resync
	pending: Vec<RecvItem>,
	// if passive, we do not send sync packets but lock on to any data stream without synchronization
	passive: bool,
}

impl fmt::Debug for Receiver {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Receiver")
			.field("half_synced", &self.half_synced)
			.field("q", &self.q)
			.field("pending", &self.pending)
			.finish()
	}
}

impl Receiver {
	fn new(state: Arc<SharedState>, max_queue_size: usize, passive: bool) -> Receiver {
		Receiver {
			state,
			buffer: None,
			q: RecvQueue::new(max_queue_size, 0u16.into()),
			half_synced: false,
			pending: Vec::new(),
			passive,
		}
	}

	async fn _handle_echo_request(&mut self) -> IoResult<()> {
		panic!("not implemented")
	}

	async fn _handle_echo_response(&mut self) -> IoResult<()> {
		panic!("not implemented")
	}

	async fn _handle_data_ack(&mut self) -> IoResult<()> {
		panic!("not implemented")
	}

	fn _update_sns(&self) {
		let max_recvd = self.q.max_consecutive_sn();
		// TODO: use correct last_recvd_sn
		let last_recvd = max_recvd;
		self.state.set_rx_sns(max_recvd, last_recvd);
	}

	async fn _send_ack<A: ToSocketAddrs>(&self, via: &UdpSocket, to: A) -> IoResult<()> {
		let hdr = self.state.compose_header(RawPacketType::DataAck);

		let mut buf = BytesMut::new();
		buf.reserve(12);
		// TODO: send full DACK packet
		hdr.write(&mut buf)?;
		let buf = buf.freeze();

		via.send_to(&buf[..], to).await?;
		Ok(())
	}

	async fn _send_echo_response<A: ToSocketAddrs>(&self, via: &UdpSocket, to: A) -> IoResult<()> {
		let hdr = self.state.compose_header(RawPacketType::EchoResponse);

		let mut buf = BytesMut::new();
		buf.reserve(12);
		hdr.write(&mut buf)?;
		let buf = buf.freeze();

		via.send_to(&buf[..], to).await?;
		Ok(())
	}

	fn _process_data<B: Buf>(&mut self, buf: &mut B) -> IoResult<bool> {
		let mut read_some = false;
		loop {
			if buf.remaining() == 0 {
				break;
			}
			let frame_hdr = RawDataFrameHeader::read(buf)?;
			let len = frame_hdr.len as usize;
			if buf.remaining() < len {
				return Err(StdIoError::new(
					StdIoErrorKind::UnexpectedEof,
					"not enough bytes remaining for data payload",
				));
			}
			let data = buf.copy_to_bytes(len);
			self.q.set(frame_hdr.sn, data);
			read_some = true;
		}
		Ok(read_some)
	}

	// Process handshake
	//
	// This, based on our local connection ID and the remote connection ID from the header and the tiebreakers (port numbers), decides how to proceed and how to synchronize the connection.
	//
	// Returns true if the synchronization was successful (either because it was pre-existing or could be done unilaterally) and false if more packets need to be transmitted and received for synchronization to happen.
	//
	// In the latter case, an Echo Request or Echo Response packet MUST be emitted in order to cause the necessary packet exchange.
	fn handshake(
		&mut self,
		local_port: u16,
		remote_port: u16,
		hdr: &RawCommonHeader,
	) -> HandshakeResult {
		let local_id = self.state.connection_id();
		let remote_id = hdr.connection_id;
		let am_tiebreaker = local_port < remote_port;

		if self.passive {
			// in passive mode, we take the ID of the remote part and claim sync immediately
			self.half_synced = false;
			if local_id != remote_id {
				self.state.change_connection_id(local_id, remote_id);
				return HandshakeResult::Resynchronized {
					lowest_sn: hdr.min_avail_sn,
				};
			} else {
				return HandshakeResult::Synchronized;
			}
		}

		if local_id != 0 {
			// we do have *some* authority over the ID at least
			if remote_id == local_id {
				if self.half_synced {
					// only now we got the "syn-ack", force a sync
					self.half_synced = false;
					HandshakeResult::Resynchronized {
						lowest_sn: hdr.min_avail_sn,
					}
				} else {
					// we know the peer, all good
					HandshakeResult::Synchronized
				}
			} else if remote_id == 0 || am_tiebreaker {
				// send more packets to break the tie
				self.half_synced = true;
				HandshakeResult::PacketsRequired
			} else {
				// need to submit to the remote to break the tie
				if !self.state.change_connection_id(local_id, remote_id) {
					// ugh, ID change did not work, retry on the next packet
					HandshakeResult::PacketsRequired
				} else {
					HandshakeResult::Resynchronized {
						lowest_sn: hdr.min_avail_sn,
					}
				}
			}
		} else if remote_id != 0 {
			// well, at least the remote knows what they’re doing
			// local_id is zero, so we learn from the remote
			if !self.state.change_connection_id(local_id, remote_id) {
				// ugh, ID change did not work, retry on the next packet
				self.half_synced = true;
				HandshakeResult::PacketsRequired
			} else {
				// we can sync right away
				HandshakeResult::Resynchronized {
					lowest_sn: hdr.min_avail_sn,
				}
			}
		} else {
			// nobody’s got any clue?
			if local_port < remote_port {
				// we are the tiebreaker, so lets roll one
				self.state.generate_connection_id();
			}
			// in either case we need to wait for more packets to confirm the choice
			// however, we need to be careful that we do actually do the synchronization of the serial number if we changed our connection ID, otherwise unspeakable things will happen.
			// assume the following flow:
			//
			// - both IDs 0
			// - remote sends DATA with ID 0
			// - local is tiebreaker, sets connection ID to 1, sends echo request
			// - remote sends echo response with ID 1: local_id == remote_id, so we claim to be synced, **but did not set lowest_sn correctly!**
			// - remote sends DATA with ID 1: we are now completely out of sync with their min_avail_sn; if we are lucky, we can advance the recvqueue, otherwise we’ll be in big trouble
			//
			// To fix this, we either need to trust that a connection ID zero is fine (which I do not like) or we need to add an intermediate state
			self.half_synced = true;
			HandshakeResult::PacketsRequired
		}
	}

	fn _resync(&mut self, lowest_sn: SerialNumber) {
		self.pending.reserve(self.q.len() + 1);
		self.pending.push(RecvItem::ResyncMarker);
		self.pending.extend(
			self.q
				.flush(lowest_sn)
				.drain(..)
				.map(|x| RecvItem::Data(x.into_payload())),
		);
		self._update_sns()
	}

	fn process_packet<B: Buf>(
		&mut self,
		local_port: u16,
		remote_port: u16,
		buf: &mut B,
	) -> IoResult<PacketResult> {
		let hdr = RawCommonHeader::read(buf)?;
		match self.handshake(local_port, remote_port, &hdr) {
			HandshakeResult::Synchronized => (),
			HandshakeResult::PacketsRequired => {
				// we need to send a packet and then wait for the reply -- so we pretend that this was an echo request :>
				// TODO: send echo response instead of dack, but the python stuff won’t like it
				return Ok(PacketResult::PacketReceived);
			}
			HandshakeResult::Resynchronized { lowest_sn } => {
				// but we can still proceed, the next receive call will just return data from the flush queue if any
				self._resync(lowest_sn)
			}
		}

		self.q.mark_unreceivable_up_to(hdr.min_avail_sn);
		self.state
			.set_remote_rx_sns(hdr.max_recvd_sn, hdr.last_recvd_sn);

		match hdr.type_ {
			RawPacketType::Data => {
				let read_some = self._process_data(buf)?;
				if read_some {
					self._update_sns();
					Ok(PacketResult::PacketReceived)
				} else {
					Ok(PacketResult::Nop)
				}
			}
			RawPacketType::DataAck => {
				// TODO: handle DACK frames; the header based acking is already done here
				Ok(PacketResult::Nop)
			}
			other => panic!("not implemented: {:?}", other),
		}
	}

	fn try_read(&mut self) -> Option<RecvItem> {
		if self.pending.len() > 0 {
			return Some(self.pending.remove(0));
		}
		self.pending.shrink_to_fit();
		self.q
			.try_read()
			.and_then(|x| Some(RecvItem::Data(x.into_payload())))
	}

	fn _restore_buffer(&mut self) {
		if self.buffer.is_none() {
			self.buffer = Some(Box::new([0u8; 65536]))
		}
	}

	async fn recv_packet(&mut self, from: &UdpSocket) -> IoResult<RecvItem> {
		let local_port = from.local_addr().unwrap().port();
		self._restore_buffer();
		loop {
			if let Some(data) = self.try_read() {
				return Ok(data);
			}

			let mut swapspace: Option<Box<[u8; 65536]>> = None;
			std::mem::swap(&mut swapspace, &mut self.buffer);
			let buffer = &mut swapspace.as_deref_mut().unwrap();
			let (sz, addr) = match from.recv_from(&mut buffer[..]).await {
				Ok(v) => v,
				Err(e) => {
					drop(buffer);
					debug_assert!(self.buffer.is_none());
					debug_assert!(swapspace.is_some());
					std::mem::swap(&mut swapspace, &mut self.buffer);
					return Err(e);
				}
			};
			let remote_port = addr.port();
			if remote_port == local_port {
				continue;
			}
			let mut buf = &buffer[..sz];
			// IMPORTANT: do not use `?` *here*, because we did not swap the
			// swapspace back yet.
			let result = self.process_packet(local_port, remote_port, &mut buf);
			drop(buf);
			debug_assert!(self.buffer.is_none());
			debug_assert!(swapspace.is_some());
			std::mem::swap(&mut swapspace, &mut self.buffer);

			// Here we can safely use `?`, because the buffer has been swapped
			// back already
			match result? {
				PacketResult::PacketReceived => {
					if !self.passive {
						self._send_ack(from, addr).await?;
					}
				}
				PacketResult::EchoRequest => {
					if !self.passive {
						self._send_echo_response(from, addr).await?;
					}
				}
				PacketResult::Nop => (),
				other => panic!("not implementd: {:?}", other),
			}
		}
	}
}

struct Transmitter {
	state: Arc<SharedState>,
	q: SendQueue,
	mss: usize,
}

impl fmt::Debug for Transmitter {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		f.debug_struct("Transmitter").finish()
	}
}

impl Transmitter {
	fn new(state: Arc<SharedState>, max_queue_size: usize) -> Transmitter {
		Transmitter {
			state,
			q: SendQueue::new(max_queue_size, get_random_u16().into()),
			mss: 1240,
		}
	}

	fn _opportunistic_discard(&mut self) {
		let (max_recvd_sn, last_recvd_sn) = self.state.remote_rx_sns();
		self.q.discard_up_to_incl(max_recvd_sn);
		self.q.discard(last_recvd_sn);
	}

	fn add_data_frame<T: Into<Bytes>>(&mut self, payload: T) {
		self._opportunistic_discard();
		self.q.push(payload);
		self.state.set_tx_sn(self.q.min_sn());
	}

	async fn trigger_data_tx(&mut self, via: &UdpSocket) -> IoResult<()> {
		self._opportunistic_discard();
		self.state.set_tx_sn(self.q.min_sn());

		if self.q.len() == 0 {
			return Ok(());
		}

		let hdr = self.state.compose_header(RawPacketType::Data);
		// most recent packet first, then we start with the oldest to increase chances of it getting delivered eventually
		let mut backing = Vec::<u8>::new();
		backing.resize(self.mss, 0u8);
		let mut buf = &mut backing[..];
		hdr.write(&mut buf)?;

		let (newest_sn, ref newest_data) = self.q[self.q.len() - 1];
		buf.put(newest_data.clone());

		for (frame_sn, frame_data) in self.q.iter() {
			if *frame_sn == newest_sn {
				break;
			}
			if frame_data.len() > buf.remaining_mut() {
				continue;
			}
			buf.put(frame_data.clone());
		}

		let peer_address = self.state.peer_address();
		let remaining = buf.remaining_mut();
		let written = backing.len() - remaining;
		let payload = &backing[..written];

		via.send_to(payload, peer_address).await?;
		Ok(())
	}
}

#[derive(Debug)]
pub struct Socket {
	state: Arc<SharedState>,
	inner: UdpSocket,
	receiver: Receiver,
	transmitter: Transmitter,
}

/// **Danger:** This protocol is vulnerable to trivial Denial of Service
/// attacks by an off-path attacker with the capability to spoof UDP source
/// addresses.
///
/// **Note:** Inbound data is only processed during a call to
/// [`Socket::recv_packet`]. That means that you should *always* be calling it
/// to avoid loss of data when kernel buffers overflow (just like with normal
/// UDP sockets).
impl Socket {
	pub fn new(conn: UdpSocket, initial_peer_addr: SocketAddr, passive: bool) -> Socket {
		let state = Arc::new(SharedState::new(initial_peer_addr));
		Socket {
			state: state.clone(),
			inner: conn,
			receiver: Receiver::new(state.clone(), 256, passive),
			transmitter: Transmitter::new(state, 256),
		}
	}

	pub async fn recv_packet(&mut self) -> IoResult<RecvItem> {
		self.receiver.recv_packet(&self.inner).await
	}

	pub async fn send_data<T: Into<Bytes>>(&mut self, payload: T) -> IoResult<()> {
		self.transmitter.add_data_frame(payload);
		self.transmitter.trigger_data_tx(&self.inner).await
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::net::IpAddr;

	fn test_receiver() -> (Arc<SharedState>, Receiver) {
		let state = Arc::new(SharedState::new(SocketAddr::new(
			"0.0.0.0".parse::<IpAddr>().unwrap(),
			7201u16,
		)));
		let r = Receiver::new(state.clone(), 16, false);
		(state, r)
	}

	// First, we test the core "handshake" state transitions.
	//
	// 1. remote id = 0 && local id = 0 (unassociated)
	//    - choose ID if we have the **lower** port number
	//    - we have no frames in the inbound queue, so we can trivially sync
	//    - send an DACK in any case to give remote side a chance to learn
	//      that we have no ID either if they have the lower port number
	// 2. remote id = 0 && local id != 0 (associated, but remote lost the connection)
	//    - remote end got disassociated, so we use our ID
	//    - if we have any frames in the inbound queue, we need to flush them
	// 3. remote id != 0 && local id = 0 (unassociated / lost connection)
	//    - use remote ID
	//    - we should have no frames in the inbound queue and can trivially sync
	// 4. remote id != 0 && local id != 0 && remote id != local id
	//    - use ID of the peer with the lower port number
	//    - if we have any frames in the inbound queue && have the lower port
	//      number, we need to flush
	//    - send DACK to proceed with sync
	//
	// in NO case must packets from the same port number be accepted, both for
	// the election but also to work safely in broadcast mode. a bit like
	// DHCP, really.
	//
	// in any case when the ID is set it is set by the receiving thread. only
	// when the ID is cleared, the transmitting thread may be involved
	// (because it notices that it hasn't gotten an ack for too long a time)

	#[test]
	fn test_handshake_both_zero_remote_tiebreaker() {
		// first scenario: both peers have no connection ID set and the remote is the tiebreaker
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		let r = receiver.handshake(
			// remote has the lower port number
			2048, 1024, &hdr,
		);
		// that requires to emit a packet so that the remote learns that they
		// have the higher port number.
		match r {
			HandshakeResult::PacketsRequired => (),
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0);
	}

	#[test]
	fn test_handshake_both_zero_local_tiebreaker() {
		// second scenario: both peers have no connection ID set, but we are the tiebreaker
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		let r = receiver.handshake(
			// local has the lower port number
			1024, 2048, &hdr,
		);
		// we have to roll a connection ID and choose it, but we still need to wait for the remote side to accept that before accepting data.
		match r {
			HandshakeResult::PacketsRequired => (),
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		let new_connection_id = state.connection_id();
		assert_ne!(new_connection_id, 0);

		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: new_connection_id,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let r = receiver.handshake(
			// port numbers should not matter now because we have a connection ID
			0, 0, &hdr,
		);

		// and now it is crucial that Resynchronized is emitted, otherwise unspeakable things will happen (because we did not properly sync the serial number)
		// as a side, this guards against off-path injection somewhat
		match r {
			HandshakeResult::Resynchronized { lowest_sn } => {
				assert_eq!(lowest_sn, 1234u16.into());
			}
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), new_connection_id);
	}

	#[test]
	fn test_handshake_both_zero_local_tiebreaker_finishes_sync() {
		// first the remote opener
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		receiver.handshake(
			// local has the lower port number
			1024, 2048, &hdr,
		);
		let new_connection_id = state.connection_id();
		// then the remote "SYN ACK"
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: new_connection_id,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		receiver.handshake(
			// port numbers should not matter now because we have a connection ID
			0, 0, &hdr,
		);
		// and now some data -- this must now be "Synchronized"
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: new_connection_id,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let r = receiver.handshake(
			// port numbers should not matter now because we have a connection ID
			0, 0, &hdr,
		);
		match r {
			HandshakeResult::Synchronized => (),
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), new_connection_id);
	}

	#[test]
	fn test_handshake_local_zero() {
		// third: remote has a connection ID, but we do not
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0x2342,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		let r = receiver.handshake(
			// port numbers don’t matter now, set them equal (forbidden!)
			0, 0, &hdr,
		);
		match r {
			// sync succeeds immediately
			HandshakeResult::Resynchronized { lowest_sn } => {
				assert_eq!(lowest_sn, 1234u16.into());
			}
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0x2342);
	}

	#[test]
	fn test_handshake_remote_zero() {
		// fourth: remote has no connection ID, but we do
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		assert!(state.change_connection_id(0, 0x2342));
		let r = receiver.handshake(
			// port numbers don’t matter now, set them equal (forbidden!)
			0, 0, &hdr,
		);
		match r {
			// we need to send more packets to the remote in order for them to accept our ID
			// we cannot accept data from the remote before that
			HandshakeResult::PacketsRequired => (),
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0x2342);

		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0x2342,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let r = receiver.handshake(
			// port numbers should not matter now because we have a connection ID
			0, 0, &hdr,
		);

		// and now it is crucial that Resynchronized is emitted, otherwise unspeakable things will happen (because we did not properly sync the serial number)
		// as a side, this guards against off-path injection somewhat
		match r {
			HandshakeResult::Resynchronized { lowest_sn } => {
				assert_eq!(lowest_sn, 1234u16.into());
			}
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0x2342);
	}

	#[test]
	fn test_handshake_both_nonzero_unequal_local_tiebreaker() {
		// fifth: both have a connection ID and they do not match and we are the tiebreaker
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0xf00d,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		assert!(state.change_connection_id(0, 0x2342));
		let r = receiver.handshake(1024, 2048, &hdr);
		match r {
			// we resynchronize, but we also need to send more packets in order to inform the remote about what is going on. before that, we cannot accept any data, so we do not resync
			HandshakeResult::PacketsRequired => (),
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0x2342);

		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0x2342,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let r = receiver.handshake(
			// port numbers should not matter now because we have a connection ID
			0, 0, &hdr,
		);

		// and now it is crucial that Resynchronized is emitted, otherwise unspeakable things will happen (because we did not properly sync the serial number)
		// as a side, this guards against off-path injection somewhat
		match r {
			HandshakeResult::Resynchronized { lowest_sn } => {
				assert_eq!(lowest_sn, 1234u16.into());
			}
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0x2342);
	}

	#[test]
	fn test_handshake_both_nonzero_unequal_remote_tiebreaker() {
		// sixth: both have a connection ID and they do not match but the remote is the tiebreaker
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0xf00d,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		assert!(state.change_connection_id(0, 0x2342));
		let r = receiver.handshake(2048, 1024, &hdr);
		match r {
			// we resynchronize based on the remote ID
			HandshakeResult::Resynchronized { lowest_sn } => {
				assert_eq!(lowest_sn, 1234u16.into());
			}
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0xf00d);
	}

	#[test]
	fn test_handshake_both_nonzero_and_equal() {
		// seventh and finally: both have the same connection ID, everything is golden
		let hdr = RawCommonHeader {
			version: PROTOCOL_VERSION,
			type_: RawPacketType::Data,
			connection_id: 0x2342,
			min_avail_sn: 1234u16.into(),
			max_recvd_sn: 65535u16.into(),
			last_recvd_sn: 65535u16.into(),
		};
		let (state, mut receiver) = test_receiver();
		assert!(state.change_connection_id(0, 0x2342));
		let r = receiver.handshake(
			// again do port numbers not matter
			0, 0, &hdr,
		);
		match r {
			HandshakeResult::Synchronized => (),
			other => panic!(
				"unexpected handshake result: {:?} on receiver {:x?} with {:x?}",
				other, receiver, hdr
			),
		}
		assert_eq!(state.connection_id(), 0x2342);
	}
}
