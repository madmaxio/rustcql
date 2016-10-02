use std::io::{
    Result,
    Error,
    ErrorKind,
    Write
};

use std::net::TcpStream;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;

use tokio_core;
use bufstream;
use bufstream::BufStream;

use shared;

use shared::{
    Request,
    Consistency,
    Response,
    BatchQuery,
    Column,
    ResultBody
};

use reading::reader::ReadMessage;
use writing::WriteMessage;

use tokio_core::reactor::Core;


macro_rules! t {
    ($e:expr) => (match $e {
        Ok(e) => e,
        Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
    })
}

pub fn tokio_connect() {
    let mut l = t!(Core::new());
    let addr = t!(SocketAddr::from_str("127.0.0.1:9042"));

    let stream = tokio_core::net::TcpStream::connect(&addr, &l.handle());
    let mut stream = t!(l.run(stream));




    let startup_msg = startup_request();
//    let mut buf = BufStream::new(stream);

    t!(stream.write_message(startup_msg));

    stream.flush();
/*
//    try!(buf.flush());

    let msg = t!(stream.read_message());

    match msg {
        Response::Ready => {
            println!("No auth required by server - moving on");
            //            let cli = Connection { buf: buf };
            //            Ok(cli)
        }
        Response::Authenticate(_) => {
            println!("Auth required - sending credentials - maybe");
            //            let cli = Connection { buf: buf };
            //            Ok(cli)
        }
        _ => {
            println!("Bad response - response was {:?}", msg);
            //            Err(Error::new(ErrorKind::ConnectionRefused, "Invalid response after startup"))
        }
    }*/
}

pub struct Connection {
    buf: BufStream<TcpStream>
}

fn startup_request() -> Request {
    let mut body = HashMap::new();
    body.insert("CQL_VERSION".to_string(), "3.4.3".to_string());

    Request::Startup(body)
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

#[test]
fn test_tokio() {
    tokio_connect();
}

//#[ignore]
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

//#[ignore]
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
    super_key bigint,
    user_id bigint,
    first varchar,
    last varchar,
    age int,
    height float,
    PRIMARY KEY (super_key, user_id)
    )".to_string();

    let response = conn.query(query, Consistency::Quorum);
    println!("Result of CREATE TABLE was {:?}", response);


    for i in 0..1000 {
        let query = "INSERT INTO testing.users (super_key, user_id, first, last, age, height)
               VALUES (1, ?, 'John', 'Smith', 42, 12.1);".to_string();
        let values = vec![shared::Column::Bigint(i)];

        let response = conn.prm_query(query, values, Consistency::Quorum).unwrap();

        println!("Result of prm_query was {:?}", response);
    }

    let query = "SELECT * FROM testing.users where super_key = 1 and user_id > ?".to_string();

    let values = vec![shared::Column::Bigint(1)];

    if let Response::Result(rb) = conn.paged_prm_query(query.clone(), values.clone(), Consistency::Quorum, 10, None).unwrap() {

        if let ResultBody::Rows(rows, paging_state) = rb {

            println!("Result of first paged_prm_query was {:?}", rows);

            let response = conn.paged_prm_query(query.clone(), values.clone(), Consistency::Quorum, 10, paging_state).unwrap();

            println!("Result of first paged_prm_query was {:?}", response);
        }
    }
}