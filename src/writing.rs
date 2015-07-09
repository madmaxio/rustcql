use std::io::{
	Write,
	Result
};

use std::collections::HashMap;

use byteorder::{WriteBytesExt, BigEndian};

use shared::{
	CQL_BINARY_PROTOCOL_VERSION,
	Request,
	QueryFlag
};


pub trait WriteMessage {
	fn write_message(&mut self, &Request) -> Result<()>;
}

impl<W: Write> WriteMessage for W {
fn write_message(&mut self, message: &Request) -> Result<()> {
	let mut header = Vec::new();

	try!(WriteBytesExt::write_u8(&mut header, CQL_BINARY_PROTOCOL_VERSION));
	try!(WriteBytesExt::write_u8(&mut header, 0x00));
	try!(WriteBytesExt::write_i16::<BigEndian>(&mut header, 1));
	try!(WriteBytesExt::write_u8(&mut header, message.opcode()));


	let mut buf = Vec::new();

	match *message {
			Request::Startup(ref hash_map) => {
			// try!(body.write(hash_map.as_cql_binary()));
				try!(buf.write_u16::<BigEndian>(hash_map.len() as u16));
				for (key, val) in hash_map.iter() {
					try!(buf.write_u16::<BigEndian>(key.len() as u16));
					try!(Write::write(&mut buf, key.as_bytes()));
					try!(buf.write_u16::<BigEndian>(val.len() as u16));
					try!(Write::write(&mut buf, val.as_bytes()));
				}
			}
			Request::Query(ref query, ref consistency) => {
				try!(buf.write_i32::<BigEndian>(query.len() as i32));
				try!(Write::write(&mut buf, query.as_bytes()));
				try!(buf.write_u16::<BigEndian>((*consistency).clone() as u16));
				try!(WriteBytesExt::write_u8(&mut buf, 0 as u8));
			}
			Request::ValuesQuery(ref query, ref consistency) => {
				try!(buf.write_i32::<BigEndian>(query.len() as i32));
				try!(Write::write(&mut buf, query.as_bytes()));
				try!(buf.write_u16::<BigEndian>((*consistency).clone() as u16));

				try!(WriteBytesExt::write_u8(&mut buf, 0 | QueryFlag::Values as u8));

				let prm = "hi".to_string();

				try!(buf.write_u16::<BigEndian>(2 as u16));

				try!(buf.write_i32::<BigEndian>(prm.len() as i32));
				try!(Write::write(&mut buf, prm.as_bytes()));

				try!(buf.write_i32::<BigEndian>(prm.len() as i32));
				try!(Write::write(&mut buf, prm.as_bytes()));
			}
			_ => ()
		}

	try!(self.write(header.as_slice()));
	try!(self.write_u32::<BigEndian>(buf.len() as u32));
	try!(self.write(buf.as_slice()));

	Ok(())
}
}