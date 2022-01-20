use std::borrow::Cow;
use std::io;
use std::path::Path;

use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};

use byteorder::{LittleEndian, WriteBytesExt};

use openat;

use crate::metric;

use super::archive::{ArchiveError, ArchiveWrite};

const TO_ESCAPE: &AsciiSet = &CONTROLS.add(b'/');

pub struct SimpleFileArchive {
	root: openat::Dir,
	dirmode: u32,
	filemode: u32,
}

fn open_create_dir<P: AsRef<Path>>(
	parent: &openat::Dir,
	name: P,
	mode: u32,
) -> io::Result<openat::Dir> {
	let name = name.as_ref();
	match parent.create_dir(name, mode) {
		Ok(_) => (),
		Err(e) if e.kind() == io::ErrorKind::AlreadyExists => (),
		Err(e) => return Err(e.into()),
	};
	parent.sub_dir(name)
}

impl SimpleFileArchive {
	pub fn new(inner: openat::Dir, mode: u32) -> Self {
		Self {
			root: inner,
			dirmode: mode | 0o111,
			filemode: mode,
		}
	}
}

impl ArchiveWrite for SimpleFileArchive {
	fn write(&mut self, block: &metric::StreamBlock) -> Result<(), ArchiveError> {
		let device_dir: Cow<'_, str> =
			utf8_percent_encode(&block.path.device_type, TO_ESCAPE).into();
		let device_dir = open_create_dir(&self.root, &*device_dir, self.dirmode)?;
		let instance_dir: Cow<'_, str> =
			utf8_percent_encode(&block.path.instance, TO_ESCAPE).into();
		let instance_dir = open_create_dir(&device_dir, &*instance_dir, self.dirmode)?;
		let filename = block
			.t0
			.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
		let mut f = instance_dir.write_file(filename, self.filemode)?;
		f.write_u8(0)?;
		f.write_i64::<LittleEndian>(block.t0.timestamp())?;
		f.write_u32::<LittleEndian>(block.t0.timestamp_subsec_nanos())?;
		f.write_u128::<LittleEndian>(block.period.as_nanos())?;
		match *block.data {
			metric::RawData::I16(ref vs) => {
				for v in vs.iter() {
					f.write_i16::<LittleEndian>(*v)?;
				}
			}
			metric::RawData::F64(ref vs) => {
				for v in vs.iter() {
					f.write_f64::<LittleEndian>(*v)?;
				}
			}
		};
		f.sync_all()?;
		Ok(())
	}
}
