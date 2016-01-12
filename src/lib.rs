#![feature(convert)]

extern crate core;
extern crate bufstream;
extern crate chrono;
extern crate byteorder;
extern crate uuid;

pub mod shared;

pub mod reading {
  pub mod reader;
  pub mod spec;
  pub mod value;
}

pub mod writing;

use std::io::{
  Result,
  Error,
  ErrorKind,
  Write
};

use std::net::TcpStream;
use std::collections::HashMap;

use bufstream::BufStream;

use shared::{
  Request,
  Consistency,
  Response,
  BatchQuery,
  Column
};


use reading::reader::ReadMessage;
use writing::WriteMessage;


fn startup_request() -> Request {
  let mut body = HashMap::new();
  body.insert("CQL_VERSION".to_string(), "3.2.0".to_string());

  Request::Startup(body)
}

pub struct Connection {
  buf: BufStream<TcpStream>
}

impl Connection {
  pub fn query(&mut self, query: String, consistency: Consistency) -> Result<Response> {
    let message = Request::Query(query, consistency);
    try!(self.buf.write_message(message));
    try!(self.buf.flush());

    Ok(try!(self.buf.read_message()))
  }
  pub fn prm_query(&mut self, query: String, values: Vec<Column>, consistency: Consistency) -> Result<Response> {
    let message = Request::PrmQuery(query, values, consistency);
    try!(self.buf.write_message(message));
    try!(self.buf.flush());

    Ok(try!(self.buf.read_message()))
  }
  pub fn prepare(&mut self, query: String) -> Result<Response> {
	  let message = Request::Prepare(query);
	  try!(self.buf.write_message(message));
	  try!(self.buf.flush());

	  Ok(try!(self.buf.read_message()))
  }
  pub fn execute(&mut self, id: Vec<u8>, values: Vec<Column>, consistency: Consistency) -> Result<Response> {
	  let message = Request::Execute(id, values, consistency);
	  try!(self.buf.write_message(message));
	  try!(self.buf.flush());

	  Ok(try!(self.buf.read_message()))
  }
  pub fn execute_batch(&mut self, queries: Vec<BatchQuery>, consistency: Consistency) -> Result<Response> {
    let message = Request::Batch(queries, consistency);
    try!(self.buf.write_message(message));
    try!(self.buf.flush());

    Ok(try!(self.buf.read_message()))
  }
}


pub fn connect(addr: String) -> Result<Connection> {

  let stream = try!(TcpStream::connect(&*addr));

  let startup_msg = startup_request();
  let mut buf = BufStream::new(stream);
  try!(buf.write_message(startup_msg));
  try!(buf.flush());

  let msg = try!(buf.read_message());
  match msg {
    Response::Ready => {
      println!("No auth required by server - moving on");
      let cli = Connection { buf: buf };
      Ok(cli)
    }
    Response::Authenticate(_) => {
      println!("Auth required - sending credentials - maybe");
      let cli = Connection { buf: buf };
      Ok(cli)
    }
    _ => {
      println!("Bad response - response was {:?}", msg);
      Err(Error::new(ErrorKind::ConnectionRefused, "Invalid response after startup"))
    }
  }
}

#[test]
#[ignore]
fn connect_and_query() {
  let mut conn = connect("127.0.0.1:9042".to_string()).unwrap();

  let result = conn.query("DROP KEYSPACE IF EXISTS testing".to_string(), Consistency::Quorum);
  println!("Result of DROP KEYSPACE was {:?}", result);

  let query = "CREATE KEYSPACE testing
               WITH replication = {
                 'class' : 'SimpleStrategy',
                 'replication_factor' : 1
               }".to_string();
  let result = conn.query(query, Consistency::Quorum);
  println!("Result of CREATE KEYSPACE was {:?}", result);

  let result = conn.query("USE testing".to_string(), Consistency::Quorum);
  println!("Result of USE was {:?}", result);

  let query = "CREATE TABLE users (
    user_id varchar PRIMARY KEY,
    first varchar,
    last varchar,
    age int,
    height float
    )".to_string();

  let result = conn.query(query, Consistency::Quorum);
  println!("Result of CREATE TABLE was {:?}", result);

  let query = "INSERT INTO users (user_id, first, last, age, height)
               VALUES ('jsmith', 'John', 'Smith', 42, 12.1);".to_string();
  let result = conn.query(query, Consistency::Quorum);
  println!("Result of INSERT was {:?}", result);

  let result = conn.query("SELECT * FROM users".to_string(), Consistency::Quorum);
  println!("Result of SELECT was {:?}", result);
}
