use std::collections::HashMap;
use core::cmp::PartialEq;



pub static CQL_BINARY_PROTOCOL_VERSION:u8 = 0x04;


pub enum QueryFlag {
	None = 0x00,
	Values = 0x01,
	SkipMetadata = 0x02,
	PageSize = 0x04,
	WithPagingState = 0x08,
	WithSerialConsistency = 0x10,
	WithDefaultTimestamp = 0x20,
	WithNamesForValues = 0x40
}

#[derive(Clone, Copy)]
pub enum Consistency {
	Any = 0x0000,
	One = 0x0001,
	Two = 0x0002,
	Three = 0x0003,
	Quorum = 0x0004,
	All = 0x0005,
	LocalQuorum = 0x0006,
	EachQuorum = 0x0007,
	Serial = 0x0008,
	LocalSerial = 0x0009,
	LocalOne = 0x000A,
	Unknown
}

pub enum BatchType {
	Logged = 0x00,
	Unlogged = 0x01,
	Counter = 0x02
}

pub enum BatchFlag {
	None = 0x00,
	WithSerialConsistency = 0x10,
	WithDefaultTimestamp = 0x20,
	WithNamesForValues = 0x40
}

pub enum BatchQueryKind {
	Simple = 0x00,
	Prepared = 0x01
}

#[derive(Debug)]
pub enum BatchQuery {
	Simple(String),
	SimpleWithParams(String, Vec<Column>),
	Prepared(Vec<u8>, Vec<Column>)
}

pub enum Request {
	Startup(HashMap<String, String>),
  	Options,
  	Query(String, Consistency),
  	PrmQuery(String, Vec<Column>, Consistency),
    PrmQueryWithNames(String, Vec<(String, Column)>, Consistency),
 	Prepare(String),
	Execute(Vec<u8>, Vec<Column>, Consistency),
	Batch(Vec<BatchQuery>, Consistency)
}

impl Request {
  pub fn opcode(&self) -> u8 {
    match *self {
      	Request::Startup(_) => 0x01,
      	Request::Options => 0x05,
      	Request::Query(_, _) | Request::PrmQuery(_, _, _) | Request::PrmQueryWithNames(_, _, _)  => 0x07,
		Request::Prepare(_) => 0x09,
		Request::Execute(_, _, _) => 0x0A,
		Request::Batch(_, _) => 0x0D
    }
  }
}

#[derive(Debug)]
pub enum Response {
  Error(u32, String),
  Ready,
  Supported,
  Result(ResultBody),
  Authenticate(String),
  Unknown,
  Empty
}

#[derive(Debug)]
pub enum ResultBody {
  Void,
  Rows(Vec<Row>),
  SetKeyspace(String),
  Prepared(Vec<u8>),
  SchemaChange(String, String, String)
}

#[derive(Debug)]
pub struct ColumnSpec {
	pub name: String,
	pub data_type: ColumnType,
	pub collection_spec: CollectionSpec
}

#[derive(Debug, Clone)]
pub enum CollectionSpec {
	None,
	Set(ColumnType),
	List(ColumnType),
	Map(ColumnType, ColumnType)
}

#[derive(Debug)]
pub struct Row {
  pub columns: HashMap<String, Column>
}

#[derive(Copy, Debug, Clone)]
pub enum ColumnType {
	Custom = 0x0000,
	Ascii = 0x0001,
	Bigint = 0x0002,
	Blob = 0x0003,
	Boolean = 0x0004,
	Counter = 0x0005,
	Decimal = 0x0006,
	Double = 0x0007,
	Float = 0x0008,
	Int = 0x0009,
	Text = 0x000A,
	Timestamp = 0x000B,
	Uuid = 0x000C,
	Varchar = 0x000D,
	Varint = 0x000E,
	Timeuuid = 0x000F,
	Inet = 0x0010,
	List = 0x0020,
	Map = 0x0021,
	Set = 0x0022,
    UDT = 0x0030,
	Tuple = 0x0031
}

pub fn to_column_type(value: u16) -> ColumnType {
	match value {		
		0x0000 => ColumnType::Custom,
		0x0001 => ColumnType::Ascii,
		0x0002 => ColumnType::Bigint,
		0x0003 => ColumnType::Blob,
		0x0004 => ColumnType::Boolean,
		0x0005 => ColumnType::Counter,
		0x0006 => ColumnType::Decimal,
		0x0007 => ColumnType::Double,
		0x0008 => ColumnType::Float,
		0x0009 => ColumnType::Int,
		0x000A => ColumnType::Text,
		0x000B => ColumnType::Timestamp,
		0x000C => ColumnType::Uuid,
		0x000D => ColumnType::Varchar,
		0x000E => ColumnType::Varint,
		0x000F => ColumnType::Timeuuid,
		0x0010 => ColumnType::Inet,
		0x0020 => ColumnType::List,
		0x0021 => ColumnType::Map,
		0x0022 => ColumnType::Set,
		0x0030 => ColumnType::UDT,
		0x0031 => ColumnType::Tuple,
		_ => ColumnType::Varchar
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Column {
	None,
	String(String),
	Int(i32),
	Bigint(i64),
	Float(f32),
	Double(f64),
	Timestamp(i64),
	Set(Vec<Column>),
	List(Vec<Column>),
	Map(Vec<(Column, Column)>)
}

impl Column {
	pub fn get_string(&self) -> Option<String> {
		match *self {
			Column::String(ref val) => Some(val.clone()),
			_ => None
		}
	}
	pub fn get_int(&self) -> Option<i32> {
		match *self {
				Column::Int(ref val) => Some(*val),
				_ => None
			}
	}
	pub fn get_bigint(&self) -> Option<i64> {
		match *self {
				Column::Bigint(ref val) => Some(*val),
				_ => None
			}
	}
	pub fn get_float(&self) -> Option<f32> {
		match *self {
				Column::Float(ref val) => Some(*val),
				_ => None
			}
	}
	pub fn get_double(&self) -> Option<f64> {
		match *self {
				Column::Double(ref val) => Some(*val),
				_ => None
			}
	}
	pub fn get_timestamp(&self) -> Option<i64> {
		match *self {
				Column::Timestamp(ref val) => Some(*val),
				_ => None
			}
	}
	pub fn get_vec(&self) -> Option<Vec<Column>> {
		match *self {
			Column::Set(ref val) | Column::List(ref val) => {
				if val.len() == 0 {
					return None;
				}
				Some(val.clone())
			}
			_ => None
		}
	}
}