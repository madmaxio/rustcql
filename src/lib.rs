#![feature(convert)]
#![feature(custom_derive, plugin)]

extern crate core;
extern crate futures;
extern crate tokio_core;
extern crate bufstream;
extern crate byteorder;
extern crate uuid;

pub mod connection;
pub mod shared;

mod reading {
  pub mod reader;
  mod spec;
  mod value;
}

pub mod writing;

use std::io::{
  Result,
  Error,
  ErrorKind,
  Write
};



pub use connection::Connection;
pub use connection::connect;

