use std::io::{
	Write,
	Result
};

use std::mem::size_of;

use byteorder::{WriteBytesExt, BigEndian};

use shared::{
	CQL_BINARY_PROTOCOL_VERSION,
	Request,
	QueryFlag,
	BatchType,
	BatchFlag,
	BatchQuery,
	BatchQueryKind,
	Column
};


pub trait WriteMessage {
	fn write_message(&mut self, Request) -> Result<()>;
}

impl<W: Write> WriteMessage for W {
fn write_message(&mut self, message: Request) -> Result<()> {
	let mut header = Vec::new();

	try!(WriteBytesExt::write_u8(&mut header, CQL_BINARY_PROTOCOL_VERSION));
	try!(WriteBytesExt::write_u8(&mut header, 0x00));
	try!(WriteBytesExt::write_u16::<BigEndian>(&mut header, 1));
	try!(WriteBytesExt::write_u8(&mut header, message.opcode()));


	let mut buf = Vec::new();

	match message {
			Request::Startup(ref hash_map) => {
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
			Request::PrmQuery(ref query, ref values, ref consistency) => {
				//println!("query is {}", query);
				try!(buf.write_i32::<BigEndian>(query.len() as i32));
				try!(Write::write(&mut buf, query.as_bytes()));
				try!(buf.write_u16::<BigEndian>((*consistency).clone() as u16));

				try!(WriteBytesExt::write_u8(&mut buf, 0 | QueryFlag::Values as u8));


				try!(buf.write_u16::<BigEndian>(values.len() as u16));

				write_values(&mut buf, values);
			}
            Request::PrmQueryWithNames(ref query, ref named_values, ref consistency) => {
                //println!("query is {}", query);
                try!(buf.write_i32::<BigEndian>(query.len() as i32));
                try!(Write::write(&mut buf, query.as_bytes()));
                try!(buf.write_u16::<BigEndian>((*consistency).clone() as u16));

                try!(WriteBytesExt::write_u8(&mut buf, 0 | QueryFlag::Values as u8 | QueryFlag::WithNamesForValues as u8));


                try!(buf.write_u16::<BigEndian>(named_values.len() as u16));

                write_named_values(&mut buf, named_values);
            }
			Request::Prepare(ref query) => {
				try!(buf.write_i32::<BigEndian>(query.len() as i32));
				try!(Write::write(&mut buf, query.as_bytes()));
			}
			Request::Execute(ref id, ref values, ref consistency) => {
				try!(buf.write_u16::<BigEndian>(id.len() as u16));
				try!(Write::write(&mut buf, id));
				try!(buf.write_u16::<BigEndian>((*consistency).clone() as u16));

				try!(WriteBytesExt::write_u8(&mut buf, 0 | QueryFlag::Values as u8));


				try!(buf.write_u16::<BigEndian>(values.len() as u16));

				write_values(&mut buf, values);
			}
			Request::Batch(ref queries, ref consistency) => {
				try!(WriteBytesExt::write_u8(&mut buf, BatchType::Logged as u8));
				try!(buf.write_u16::<BigEndian>(queries.len() as u16));
				for query in queries.iter() {
					match query {
						&BatchQuery::Simple(ref query) => {
							try!(WriteBytesExt::write_u8(&mut buf, BatchQueryKind::Simple as u8));
							try!(buf.write_i32::<BigEndian>(query.len() as i32));
							try!(Write::write(&mut buf, query.as_bytes()));
							try!(buf.write_u16::<BigEndian>(0 as u16));
						}
						&BatchQuery::SimpleWithParams(ref query, ref values) => {
							//println!("query is {}", query);
							try!(WriteBytesExt::write_u8(&mut buf, BatchQueryKind::Simple as u8));
							try!(buf.write_i32::<BigEndian>(query.len() as i32));
							try!(Write::write(&mut buf, query.as_bytes()));

							try!(buf.write_u16::<BigEndian>(values.len() as u16));

							write_values(&mut buf, values);
						}
						&BatchQuery::Prepared(ref id, ref values) => {
							try!(WriteBytesExt::write_u8(&mut buf, BatchQueryKind::Prepared as u8));
							try!(buf.write_u16::<BigEndian>(id.len() as u16));
							try!(Write::write(&mut buf, id));

							try!(buf.write_u16::<BigEndian>(values.len() as u16));

							write_values(&mut buf, values);
						}
					}
				}
				try!(buf.write_u16::<BigEndian>((*consistency).clone() as u16));
				try!(WriteBytesExt::write_u8(&mut buf, BatchFlag::None as u8));
			}
			_ => ()
		}

	try!(self.write(header.as_slice()));
	try!(self.write_u32::<BigEndian>(buf.len() as u32));
	try!(self.write(buf.as_slice()));

	Ok(())
    }
}

fn write_values(buf: &mut Vec<u8>, values: &Vec<Column>) -> Result<()> {
	for col in values.iter() {
		try!(buf.write_i32::<BigEndian>(value_size(col) as i32));
		write_value(buf, col);
	}
	Ok(())
}

fn write_named_values(buf: &mut Vec<u8>, named_values: &Vec<(String, Column)>) -> Result<()> {
    for &(ref name, ref col) in named_values.iter() {
        try!(buf.write_u16::<BigEndian>(name.len() as u16));
        try!(Write::write(buf, name.as_bytes()));
        try!(buf.write_i32::<BigEndian>(value_size(col) as i32));
        write_value(buf, col);
    }
    Ok(())
}

fn value_size(value: &Column) -> usize {
	match value {
		&Column::String(ref v) => v.len(),
		&Column::Int(ref v) => size_of::<i32>(),
		&Column::Bigint(ref v) => size_of::<i64>(),
		&Column::Float(ref v) => size_of::<f32>(),
		&Column::Double(ref v) => size_of::<f64>(),
		&Column::Timestamp(ref v) => size_of::<i64>(),
		&Column::Set(ref v) | &Column::List(ref v) => size_of::<i32>() + ((*v).len()) * (value_size(&(*v)[0]) + size_of::<i32>()),
		_ => 0
	}
}

fn write_value(buf: &mut Vec<u8>, value: &Column) -> Result<()> {
	match value {
		&Column::String(ref v) => {try!(Write::write(buf, v.as_bytes()));}
		&Column::Int(ref v) => {try!(buf.write_i32::<BigEndian>(*v));}
		&Column::Bigint(ref v) => {try!(buf.write_i64::<BigEndian>(*v));}
		&Column::Float(ref v) => {try!(buf.write_f32::<BigEndian>(*v));}
		&Column::Double(ref v) => {try!(buf.write_f64::<BigEndian>(*v));}
		&Column::Timestamp(ref v) => {try!(buf.write_i64::<BigEndian>(*v));}
		&Column::Set(ref v) | &Column::List(ref v) => {
			try!(buf.write_i32::<BigEndian>((*v).len() as i32));
			for value in (*v).iter() {
				try!(buf.write_i32::<BigEndian>(value_size(value) as i32));
				write_value(buf, value);
			}
		},
		_ => {}
	}
	Ok(())
}