use std::{net::TcpStream, sync::{Mutex, Arc}};
use anyhow::Result;

/// get the fully qualified domain name for a function from its name and version
pub fn calculate_fqdn(function_name: &String, function_version: &String) -> String {
  format!("{}-{}", function_name, function_version)
}

pub fn calculate_invoke_uri(address: &str, port: Port) -> String {
  format!("http://{}:{}/invoke", address, port)
}

pub fn calculate_base_uri(address: &str, port: Port) -> String {
  format!("http://{}:{}/", address, port)
}

pub const TEMP_DIR: &str = "/tmp/ilúvatar_worker";

pub fn temp_file(with_tail: &String, with_extension: &str) -> Result<String> {
  let ret = format!("{}/{}.{}", TEMP_DIR, with_tail, with_extension);
  touch(&ret)?;
  return Ok(ret);
}

// A simple implementation of `% touch path` (ignores existing files)
fn touch(path: &String) -> std::io::Result<()> {
  match std::fs::OpenOptions::new().create(true).write(true).open(path) {
      Ok(_) => Ok(()),
      Err(e) => Err(e),
  }
}

pub fn ensure_temp_dir() -> Result<()> {
  std::fs::create_dir_all(TEMP_DIR)?;
  Ok(())
}

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
  let mut lock = NEXT_PORT_MUTEX.lock().unwrap();
  let mut try_port = *lock;
  while ! is_port_free(try_port) {
    try_port += 1;
    if try_port >= MAX_PORT {
      try_port = START_PORT;
    }
  }
  let ret = Ok(try_port);
  *lock = try_port+ 1;
  return ret;
}


#[cfg(test)]
mod tests {
  use super::*;
  use rstest::rstest;

  #[rstest]
  #[case("localhost", 8080, "http://localhost:8080/invoke")]
  #[case("localhost", 8081, "http://localhost:8081/invoke")]
  #[case("localhost", 19840, "http://localhost:19840/invoke")]
  #[case("0.0.0.0", 8080, "http://0.0.0.0:8080/invoke")]
  fn format_invoke_correctly(#[case] addr: &str, #[case] port: Port, #[case] expected: &str){
    let ans = calculate_invoke_uri(addr, port);
    assert_eq!(expected, ans);
  }

  #[rstest]
  #[case("localhost", 8080, "http://localhost:8080/")]
  #[case("localhost", 8081, "http://localhost:8081/")]
  #[case("localhost", 19840, "http://localhost:19840/")]
  #[case("0.0.0.0", 8080, "http://0.0.0.0:8080/")]
  fn format_base_correctly(#[case] addr: &str, #[case] port: Port, #[case] expected: &str){
    let ans = calculate_base_uri(addr, port);
    assert_eq!(expected, ans);
  }

  #[rstest]
  #[case("hello", "080", "hello-080")]
  #[case("cnn", "1.0.2", "cnn-1.0.2")]
  #[case("video", "1.5.2", "video-1.5.2")]
  #[case("alpine", "0.0.1", "alpine-0.0.1")]
  fn format_fqdn(#[case] name: &str, #[case] version: &str, #[case] expected: &str){
    let ans = calculate_fqdn(&name.to_string(), &version.to_string());
    assert_eq!(expected, ans);
  }
  
}