#![feature(convert)]
#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate core;
extern crate bufstream;
extern crate byteorder;
extern crate uuid;
extern crate serde;

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
  body.insert("CQL_VERSION".to_string(), "3.4.2".to_string());

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
    pub fn prm_query_with_names(&mut self, query: String, named_values: Vec<(String, Column)>, consistency: Consistency) -> Result<Response> {
        let message = Request::PrmQueryWithNames(query, named_values, consistency);
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



    pub fn paged_query(&mut self, query: String, consistency: Consistency, result_page_size: i32, paging_state: Option<Vec<u8>>) -> Result<Response> {
        let message = Request::PagedQuery(query, consistency, result_page_size, paging_state);
        try!(self.buf.write_message(message));
        try!(self.buf.flush());

        Ok(try!(self.buf.read_message()))
    }
    pub fn paged_prm_query(&mut self, query: String, values: Vec<Column>, consistency: Consistency, result_page_size: i32, paging_state: Option<Vec<u8>>) -> Result<Response> {
        let message = Request::PagedPrmQuery(query, values, consistency, result_page_size, paging_state);
        try!(self.buf.write_message(message));
        try!(self.buf.flush());

        Ok(try!(self.buf.read_message()))
    }
    pub fn paged_prm_query_with_names(&mut self, query: String, named_values: Vec<(String, Column)>, consistency: Consistency, result_page_size: i32, paging_state: Option<Vec<u8>>) -> Result<Response> {
        let message = Request::PagedPrmQueryWithNames(query, named_values, consistency, result_page_size, paging_state);
        try!(self.buf.write_message(message));
        try!(self.buf.flush());

        Ok(try!(self.buf.read_message()))
    }
    pub fn paged_execute(&mut self, id: Vec<u8>, values: Vec<Column>, consistency: Consistency, result_page_size: i32, paging_state: Option<Vec<u8>>) -> Result<Response> {
        let message = Request::PagedExecute(id, values, consistency, result_page_size, paging_state);
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

#[ignore]
#[test]
fn test_query() {
    let mut conn = connect("127.0.0.1:9042".to_string()).unwrap();

    let response = conn.query("DROP KEYSPACE IF EXISTS testing".to_string(), Consistency::Quorum);
    println!("Result of DROP KEYSPACE was {:?}", response);

    let query = "CREATE KEYSPACE testing
               WITH replication = {
                 'class' : 'SimpleStrategy',
                 'replication_factor' : 1
               }".to_string();
    let response = conn.query(query, Consistency::Quorum);
    println!("Result of CREATE KEYSPACE was {:?}", response);

    let query = "CREATE TABLE testing.users (
    user_id varchar PRIMARY KEY,
    first varchar,
    last varchar,
    age int,
    height float
    )".to_string();

    let response = conn.query(query, Consistency::Quorum);
    println!("Result of CREATE TABLE was {:?}", response);

    let query = "INSERT INTO testing.users (user_id, first, last, age, height)
               VALUES ('jsmith', 'John', 'Smith', 42, 12.1);".to_string();
    let response = conn.query(query, Consistency::Quorum);
    println!("Result of INSERT was {:?}", response);

    let response = conn.query("SELECT * FROM testing.users".to_string(), Consistency::Quorum);
    println!("Result of SELECT was {:?}", response);

    let query = "SELECT * FROM testing.users where user_id = ?".to_string();
    let values = vec![shared::Column::String("jsmith".to_string())];

    let response = conn.prm_query(query, values, Consistency::Quorum).unwrap();

    println!("Result of prm_query was {:?}", response);

    let query = "SELECT * FROM testing.users where user_id = :user_id".to_string();
    let named_values = vec![("user_id".to_string(), shared::Column::String("jsmith".to_string()))];

    let response = conn.prm_query_with_names(query, named_values, Consistency::Quorum).unwrap();

    println!("Result of prm_query_with_names was {:?}", response);
}

#[test]
fn test_paging() {
    let mut conn = connect("127.0.0.1:9042".to_string()).unwrap();

    let response = conn.query("DROP KEYSPACE IF EXISTS testing".to_string(), Consistency::Quorum);
    println!("Result of DROP KEYSPACE was {:?}", response);

    let query = "CREATE KEYSPACE testing
               WITH replication = {
                 'class' : 'SimpleStrategy',
                 'replication_factor' : 1
               }".to_string();
    let response = conn.query(query, Consistency::Quorum);
    println!("Result of CREATE KEYSPACE was {:?}", response);

    let query = "CREATE TABLE testing.users (
    user_id bigint PRIMARY KEY,
    first varchar,
    last varchar,
    age int,
    height float
    )".to_string();

    let response = conn.query(query, Consistency::Quorum);
    println!("Result of CREATE TABLE was {:?}", response);

    let query = "INSERT INTO testing.users (user_id, first, last, age, height)
               VALUES ('jsmith', 'John', 'Smith', 42, 12.1);".to_string();
    let response = conn.query(query, Consistency::Quorum);
    println!("Result of INSERT was {:?}", response);

    for i in 0..1000 {
        let query = "INSERT INTO testing.users (user_id, first, last, age, height)
               VALUES (?, 'John', 'Smith', 42, 12.1);".to_string();
        let values = vec![shared::Column::Bigint(i)];

        let response = conn.prm_query(query, values, Consistency::Quorum).unwrap();

        println!("Result of prm_query was {:?}", response);
    }

    let query = "SELECT * FROM testing.users".to_string();

    let response = conn.paged_query(query, Consistency::Quorum, 10, None).unwrap();

    if let Response::Result(payload) = response {
        if let ResultBody::Rows(rows, paging_state) = payload {

        }
    }


    //println!("Result of paged_query was {:?}", response);
}
