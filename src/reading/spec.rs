use std::io::Read;

use std::num::FromPrimitive;

use byteorder::{
	BigEndian,
	ReadBytesExt
};

use shared::{
	ColumnSpec,
	CollectionSpec,
	ColumnType
};

use reading::reader::read_fixed;

pub fn read_column_specs(buf: &mut Read, column_count: u32) -> Vec<ColumnSpec> {
	let mut column_specs = vec!();
	for _ in 0..column_count {
		let len = buf.read_u16::<BigEndian>().unwrap();
		let bytes = read_fixed(buf, len as usize);
		match3(buf, String::from_utf8(bytes).unwrap(), &mut column_specs);
	}	
	column_specs
}

fn match3(buf: &mut Read, column_name: String, column_specs: &mut Vec<ColumnSpec>) {
	let column_type_u16 = buf.read_u16::<BigEndian>().unwrap();
	let column_type_option: Option<ColumnType> = FromPrimitive::from_u16(column_type_u16);
	match column_type_option {
		Some(column_type) => {
			let mut spec = ColumnSpec {
				name: column_name,
				data_type: column_type,
				collection_spec: CollectionSpec::None
			};
			match column_type {
				ColumnType::Set | ColumnType::List | ColumnType::Map =>
					match2(buf, column_type, &mut spec),
				_ => {}
			}
			println!("Dat spec: {:?}", spec);
			column_specs.push(spec);
		}
		None => { println!("Spec not pushed to specs because column type not found in ColumnType enum"); }
	}
}

fn match2(buf: &mut Read, column_type: ColumnType, spec: &mut ColumnSpec) {
	let coll_col_type_u16 = buf.read_u16::<BigEndian>().unwrap();
	let coll_col_type_option: Option<ColumnType> = FromPrimitive::from_u16(coll_col_type_u16);
	match coll_col_type_option {
		Some(coll_col_type) => match1(buf, column_type, coll_col_type, spec),
		None => {}
	}
}

fn match1(buf: &mut Read, column_type: ColumnType, collection_column_type: ColumnType, spec: &mut ColumnSpec) {
	match column_type {
		ColumnType::Set =>
			spec.collection_spec = CollectionSpec::Set(collection_column_type),
		ColumnType::List =>
			spec.collection_spec = CollectionSpec::List(collection_column_type),
		ColumnType::Map => {
			let map_value_type_u16 = buf.read_u16::<BigEndian>().unwrap();
			let map_value_type_option: Option<ColumnType> = FromPrimitive::from_u16(map_value_type_u16);
			match map_value_type_option {
				Some(map_value_type) => {
					spec.collection_spec = CollectionSpec::Map(collection_column_type, map_value_type);
				}
				None => {}
			}
		}
		_ => {}
	}
}