use std::io::{
	Read,
	BufReader,
	Result
};

use std::collections::HashMap;

use byteorder::{
	BigEndian,
	ReadBytesExt
};

use shared::{
	Response,
	ResultBody,
	Row,
    Opcode,
    to_opcode,
    ResultKind,
    to_result_kind,
    RowsFlag
};

use reading::spec::read_column_specs;
use reading::value::read_column_value;


pub trait ReadMessage {
    fn read_message(&mut self) -> Result<Response>;
}

impl<R: Read> ReadMessage for R {
    fn read_message(&mut self) -> Result<Response> {
        let mut buf = BufReader::new(self);
        let version = try!(buf.read_u8());
        let flags = try!(buf.read_u8());
        let stream = try!(buf.read_u16::<BigEndian>());
        let opcode = to_opcode(try!(buf.read_u8()));

        try!(buf.read_u32::<BigEndian>()); // length


        let ret = match opcode {
                Opcode::Error => try!(read_error_response(&mut buf)),
                Opcode::Ready => Response::Ready,
                Opcode::Authenticate => Response::Authenticate("test".to_string()),
                Opcode::Supported => Response::Supported,
                Opcode::Result => try!(read_result(&mut buf)),
                _ => Response::Empty
            };

        Ok(ret)
    }
}

pub fn read_fixed(rdr: &mut Read, len: usize) -> Vec<u8> {		
    let mut buf = vec![0; len];    
    let mut nread = 0usize;
    while nread < buf.len() {
        match rdr.read(&mut buf[nread..]) {
            Ok(n) => {
            	match n {
            		0 => {}
            		n => nread += n
            	}
            }
            _ => {}
        }
    }
    buf
}

fn read_error_response(buf: &mut Read) -> Result<Response> {	
	let code = try!(buf.read_u32::<BigEndian>());
	let len = try!(buf.read_u16::<BigEndian>());
	let string_bytes = read_fixed(buf, len as usize);
	let res = String::from_utf8(string_bytes);
	Ok(match res {
			Ok(string) => Response::Error(code, string),
			Err(_) => Response::Error(code, "couldn't parse".to_string())
		})
}

fn read_result(buf: &mut Read) -> Result<Response> {
	let result_kind = to_result_kind(try!(buf.read_u32::<BigEndian>()));

	let body = match result_kind {
			ResultKind::Rows => {

                let flags = try!(buf.read_i32::<BigEndian>());

                let columns_count = try!(buf.read_i32::<BigEndian>());

                let mut paging_state: Option<Vec<u8>> = None;

                if flags & RowsFlag::HasMorePages as i32 == RowsFlag::HasMorePages as i32 {

                    println!("The flags are {}, and column count is {}", flags, columns_count);

                    let len = try!(buf.read_i32::<BigEndian>());
                    paging_state = Some(read_fixed(buf, len as usize));
                }

                //assume global column spec
                let len = try!(buf.read_u16::<BigEndian>());
                let bytes = read_fixed(buf, len as usize);
                let keyspace = String::from_utf8(bytes).unwrap();

                let len = try!(buf.read_u16::<BigEndian>());
                let string_bytes = read_fixed(buf, len as usize);
                let table = String::from_utf8(string_bytes).unwrap();

                //println!("The flags are {}, and column count is {}", flags, columns_count);
                //println!("The keyspace is {}, and table is {}", keyspace, table);

                let column_specs = read_column_specs(buf, columns_count);
                let row_count = try!(buf.read_i32::<BigEndian>());
                let mut rows = vec!();
                //println!("Row count: {}", row_count);

                for _ in 0..row_count {
                    let mut columns = HashMap::new();
                    for col_spec in column_specs.iter() {
                        //println!("started column {:?}", col_spec);
                        columns.insert(col_spec.name.clone(), read_column_value(buf, col_spec.data_type, col_spec.collection_spec.clone()));
                        //println!("finished column");
                    }
                    rows.push(Row { columns: columns});
                }
                ResultBody::Rows(rows, paging_state)
			}
            ResultKind::SetKeyspace => {
				let len = try!(buf.read_u16::<BigEndian>());
				let string_bytes = read_fixed(buf, len as usize);
				let name = String::from_utf8(string_bytes).unwrap();
				ResultBody::SetKeyspace(name)
			}
            ResultKind::Prepared => {
				let len = try!(buf.read_u16::<BigEndian>());
				let id = read_fixed(buf, len as usize);
				let flags = try!(buf.read_i32::<BigEndian>());
				//println!("flags is {}", flags);
				let columns_count = try!(buf.read_i32::<BigEndian>());
				//println!("columns_count is {}", columns_count);
				let pk_count = try!(buf.read_i32::<BigEndian>());
				//println!("pk_count is {}", pk_count);

				for _ in 0..pk_count {
					let pk_index = try!(buf.read_u16::<BigEndian>());
				}

				//assume global column spec

				let len = try!(buf.read_u16::<BigEndian>());
				let bytes = read_fixed(buf, len as usize);
				let keyspace = String::from_utf8(bytes).unwrap();

				let len = try!(buf.read_u16::<BigEndian>());
				let string_bytes = read_fixed(buf, len as usize);
				let table = String::from_utf8(string_bytes).unwrap();

				//println!("The flags are {}, and column count is {}", flags, columns_count);
				//println!("The keyspace is {}, and table is {}", keyspace, table);

				let column_specs = read_column_specs(buf, columns_count);
				ResultBody::Prepared(id)
			}
            ResultKind::Schema_change => {
                // dedup this - map over range?
                let len = try!(buf.read_u16::<BigEndian>());
                let string_bytes = read_fixed(buf, len as usize);
                let change = String::from_utf8(string_bytes).unwrap();

                let len = try!(buf.read_u16::<BigEndian>());
                let string_bytes = read_fixed(buf, len as usize);
                let keyspace = String::from_utf8(string_bytes).unwrap();

                let len = try!(buf.read_u16::<BigEndian>());
                let string_bytes = read_fixed(buf, len as usize);
                let table = String::from_utf8(string_bytes).unwrap();
                ResultBody::SchemaChange(change, keyspace, table)
            }
			_ => ResultBody::Void,
		};

	Ok(Response::Result(body))
}