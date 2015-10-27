use std::io::Read;

use chrono::*;

use uuid::Uuid;

use byteorder::{
	BigEndian,
	ReadBytesExt
};

use shared::{
	ColumnType,
	Column,
	CollectionSpec
};

use reading::reader::read_fixed;

pub fn read_column_value(buf: &mut Read, data_type: ColumnType, collection_spec: CollectionSpec) -> Column {

	let len = buf.read_i32::<BigEndian>().unwrap();
	//println!("num of bytes for col {:?} is {}", data_type, len);

	if len < 0 {
		//println!("read_column_value is returning without readind data (length was {})", len);
		return Column::None
	}

	match data_type {
		ColumnType::Float =>
			Column::CqlFloat(buf.read_f32::<BigEndian>().unwrap()),
		ColumnType::Double =>
			Column::CqlDouble(buf.read_f64::<BigEndian>().unwrap()),
		ColumnType::Int =>
			Column::CqlInt(buf.read_i32::<BigEndian>().unwrap()),
		ColumnType::Bigint =>
			Column::CqlBigint(buf.read_i64::<BigEndian>().unwrap()),
		ColumnType::Timeuuid => {
			let bytes = read_fixed(buf, len as usize);
			let uuid = Uuid::from_bytes(bytes.as_slice()).unwrap();
			Column::CqlString(uuid.to_hyphenated_string())
		}
		ColumnType::Timestamp =>
			Column::CqlTimestamp(get_tm(buf.read_i64::<BigEndian>().unwrap())),

		ColumnType::Set => {

			let set_len = buf.read_i32::<BigEndian>().unwrap();
			//println!("set len is {}", set_len);

			match collection_spec {
				CollectionSpec::Set(set_column_type) => {
					let mut set = vec!();
					for i in 0..set_len {
						set.push(read_collection_column_value(buf, set_column_type));
						//println!("iterating over set, counter is {}", i);
					}
					Column::Set(set)
				}
				_ => Column::None
			}

		}

		ColumnType::List => {

			let list_len = buf.read_i32::<BigEndian>().unwrap();
			//println!("list len is {}", list_len);

			match collection_spec {
				CollectionSpec::List(list_column_type) => {
					let mut list = vec!();
						for i in 0..list_len {
							list.push(read_collection_column_value(buf, list_column_type));
							//println!("iterating over list, counter is {}", i);
						}
					Column::List(list)
				},
				_ => Column::None
			}

		}

		ColumnType::Map => {

			let map_len = buf.read_i32::<BigEndian>().unwrap();
			//println!("map len is {}", map_len);

			match collection_spec {
				CollectionSpec::Map(map_key_column_type, map_value_column_type) => {
					let mut map = vec!();
					let mut key: Column;
					for i in 0..map_len {
						key = read_collection_column_value(buf, map_key_column_type);
						match key {
							Column::None => {},
							_ => map.push((key.clone(), read_collection_column_value(buf, map_value_column_type)))
						}
						//println!("iterating over map, counter is {}", i);
					}
					Column::Map(map)
				}
				_ => Column::None//Err("Wrong collection type in collection spec")

			}

		}

		_ => {
			let bytes = read_fixed(buf, len as usize);
			Column::CqlString(String::from_utf8(bytes).unwrap())
		}
	}
}

fn read_collection_column_value(buf: &mut Read, data_type: ColumnType) -> Column {

	let len = buf.read_i32::<BigEndian>().unwrap();
	//println!("num of bytes for col {:?} is {}", data_type, len);

	if len < 0 {
		//println!("read_column_value is returning without readind data (length was {})", len);
		return Column::None
	}

	match data_type {
		ColumnType::Float => Column::CqlFloat(buf.read_f32::<BigEndian>().unwrap()),
		ColumnType::Double => Column::CqlDouble(buf.read_f64::<BigEndian>().unwrap()),
		ColumnType::Int => Column::CqlInt(buf.read_i32::<BigEndian>().unwrap()),
		ColumnType::Timeuuid => {
			let bytes = read_fixed(buf, len as usize);
			Column::CqlString(Uuid::from_bytes(bytes.as_slice()).unwrap().to_hyphenated_string())
		}
		ColumnType::Timestamp =>
			Column::CqlTimestamp(get_tm(buf.read_i64::<BigEndian>().unwrap())),
		_ => {
			let bytes = read_fixed(buf, len as usize);
			Column::CqlString(String::from_utf8(bytes).unwrap())
		}
	}

}

fn get_tm(ts: i64) -> DateTime<UTC> {
	let s = (ts / 1000) as i64;
	let ns = (ts % 1000) as u32;
	let naive = NaiveDateTime::from_timestamp(s, ns);
	DateTime::from_utc(naive, UTC)
}