/*!
# Local buffer for streamed data

## On high-frequency streams and timestamps

The high frequency (>= 1Hz) streams gathered by the sensor block have one
common issue: They cannot be accurately timestamped by a real-time clock,
because no real-time clock with sufficient precision is available on the
MCU, but also because of unknown latencies between the sample acquisition
and sample processing.

However, we have an accurate serial number (16 bit) for each sample; more
0specifically, the first sample in a batch of streamed samples is
timestamped with a serial number and all subsequent samples are known to
have been acquired continuously from the source.

The "current" sequence number is also provided by the MCU on status update
frames, which means that we can correlate it with the relative "uptime"
(16 bit) millisecond timestamp, which is in turn convertible into a RTC clock
timestamp.

This conversion via the RTCifier is not perfect; because it is based by
sampling a second-precision RTC with the millisecond-prescision uptime
timestamps (plus some jitter from the packet pipeline from the MCU to the
ESP8266), it takes quite a bit of uptime to get an accurate mapping. During
this time, there is significant drift of the clock. Once a useful
*/

mod archive;
mod buffer;
#[cfg(feature = "stream-filearchive")]
mod filearchive;

pub use archive::{ArchiveError, ArchiveWrite};
pub use buffer::{InMemoryBuffer, StreamBuffer, WriteError};
#[cfg(feature = "stream-filearchive")]
pub use filearchive::SimpleFileArchive;
