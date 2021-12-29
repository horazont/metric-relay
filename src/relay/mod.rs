//! # Relay Protocol
//!
//! To forward data from one metric-relay node to another, a custom TCP-based
//! protocol is used. The protocol supports health checking and transparent
//! reconnection.
//!
//! The recipient side uses a TcpListener to wait for incoming streams. It
//! supports an arbitrary amount of incoming streams, and there is no
//! authentication going on whatsoever :).
//!
//! When a peer goes silent for a sufficient amount of time (soft timeout), an
//! in-band ping is sent which should provoke the peer to send data. If no
//! data is received after another amount of time (hard timeout), the
//! connection is closed to save resources.
//!
//! Yet, to avoid loss of data, a small amount of information is retained for
//! an even longer timeout from peers of which data has been received (session
//! timeout). This is essentially a counter which indicates the last data
//! frame received and this can be used by peers to retransmit information
//! after a connection has been reestablished.
pub mod frame;
pub mod socket;

pub use frame::DataFrame;
pub use socket::{RecvSocket, SendSocket, SessionConfig};
