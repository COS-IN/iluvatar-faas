use std::{net::TcpStream, sync::{Mutex, Arc}};
use anyhow::Result;
use crate::bail_error;

pub type Port = u16;

static MAX_PORT: Port = 65500;
static START_PORT: Port = 10000;
lazy_static::lazy_static! {
  static ref NEXT_PORT_MUTEX: Arc<Mutex<Port>> = Arc::new(Mutex::new(START_PORT));
}

fn is_port_free(port_num: Port) -> bool {
  match TcpStream::connect(("0.0.0.0", port_num)) {
    Ok(_) => true,
    Err(_) => false,
  }
}

/// Get a port number that (should) be valid
///   could be sniped by somebody else, 
///   but successive calls to this will not cause that
pub fn new_port() -> Result<Port> {
  let mut lock = match NEXT_PORT_MUTEX.lock() {
    Ok(l) => l,
    Err(e) => bail_error!(error=%e, "Failed to get lock"),
  };
  let mut try_port = *lock;
  while ! is_port_free(try_port) {
    try_port += 1;
    if try_port >= MAX_PORT {
      try_port = START_PORT;
    }
  }
  let ret = Ok(try_port);
  *lock = try_port+ 1;
  ret
}