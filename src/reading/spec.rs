use std::io::Read;

use byteorder::{
	BigEndian,
	ReadBytesExt
};

use shared::{
	ColumnSpec,
	CollectionSpec,
	ColumnType,
	to_column_type
};

use reading::reader::read_fixed;

pub fn read_column_specs(buf: &mut Read, column_count: i32) -> Vec<ColumnSpec> {
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
	let column_type = to_column_type(column_type_u16);
	let mut spec = ColumnSpec {
		name: column_name,
		data_type: column_type,
		collection_spec: CollectionSpec::None
	};
	match column_type {
		ColumnType::Set | ColumnType::List | ColumnType::Map =>
			match2(buf, column_type, &mut spec),
		_ => {}
	};
	//println!("Dat spec: {:?}", spec);
	column_specs.push(spec);
}

fn match2(buf: &mut Read, column_type: ColumnType, spec: &mut ColumnSpec) {
	let coll_col_type_u16 = buf.read_u16::<BigEndian>().unwrap();
	let coll_col_type = to_column_type(coll_col_type_u16);
	match1(buf, column_type, coll_col_type, spec);
}

fn match1(buf: &mut Read, column_type: ColumnType, collection_column_type: ColumnType, spec: &mut ColumnSpec) {
	match column_type {
		ColumnType::Set =>
			spec.collection_spec = CollectionSpec::Set(collection_column_type),
		ColumnType::List =>
			spec.collection_spec = CollectionSpec::List(collection_column_type),
		ColumnType::Map => {
			let map_value_type_u16 = buf.read_u16::<BigEndian>().unwrap();
			let map_value_type = to_column_type(map_value_type_u16);
			spec.collection_spec = CollectionSpec::Map(collection_column_type, map_value_type);
		}
		_ => {}
	}
}