use std::collections::HashMap;
use core::cmp::PartialEq;



pub static CQL_BINARY_PROTOCOL_VERSION:u8 = 0x04;



pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
    UnknownOpcode
}

pub fn to_opcode(value: u8) -> Opcode {
    match value {
        0x00 => Opcode::Error,
        0x01 => Opcode::Startup,
        0x02 => Opcode::Ready,
        0x03 => Opcode::Authenticate,
        0x05 => Opcode::Options,
        0x06 => Opcode::Supported,
        0x07 => Opcode::Query,
        0x08 => Opcode::Result,
        0x09 => Opcode::Prepare,
        0x0A => Opcode::Execute,
        0x0B => Opcode::Register,
        0x0C => Opcode::Event,
        0x0D => Opcode::Batch,
        0x0E => Opcode::AuthChallenge,
        0x0F => Opcode::AuthResponse,
        0x10 => Opcode::AuthSuccess,
        _ => Opcode::UnknownOpcode
    }
}

pub enum FrameFlag {
    None = 0x00,
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warning = 0x08,
    UseBeta = 0x10
}

pub enum ResultKind {
    Void = 0x0001,
    Rows = 0x0002,
    SetKeyspace = 0x0003,
    Prepared = 0x0004,
    Schema_change = 0x0005,
    UnknownResult
}

pub fn to_result_kind(value: u32) -> ResultKind {
    match value {
        0x0001 => ResultKind::Void,
        0x0002 => ResultKind::Rows,
        0x0003 => ResultKind::SetKeyspace,
        0x0004 => ResultKind::Prepared,
        0x0005 => ResultKind::Schema_change,
        _ => ResultKind::UnknownResult
    }
}

pub enum RowsFlag {
    None = 0x0000,
    GlobalTablesSpec = 0x0001,
    HasMorePages = 0x0002,
    NoMetadata = 0x0004
}

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
	Batch(Vec<BatchQuery>, Consistency),

    PagedQuery(String, Consistency, i32, Option<Vec<u8>>),
    PagedPrmQuery(String, Vec<Column>, Consistency, i32, Option<Vec<u8>>),
    PagedPrmQueryWithNames(String, Vec<(String, Column)>, Consistency, i32, Option<Vec<u8>>),
    PagedExecute(Vec<u8>, Vec<Column>, Consistency, i32, Option<Vec<u8>>)
}

impl Request {
  pub fn opcode(&self) -> u8 {
    match *self {
      	Request::Startup(_) => 0x01,
      	Request::Options => 0x05,
      	Request::Query(_, _) | Request::PrmQuery(_, _, _) | Request::PrmQueryWithNames(_, _, _) |
        Request::PagedQuery(_, _, _, _) | Request::PagedPrmQuery(_, _, _, _, _) | Request::PagedPrmQueryWithNames(_, _, _, _, _)
        => 0x07,
		Request::Prepare(_) => 0x09,
		Request::Execute(_, _, _) | Request::PagedExecute(_, _, _, _, _) => 0x0A,
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
  Rows(Vec<Row>, Option<Vec<u8>>),
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