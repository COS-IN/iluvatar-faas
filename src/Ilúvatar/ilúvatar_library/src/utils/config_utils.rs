extern crate clap;

use std::str::FromStr;

use clap::{ArgMatches, value_t};

pub fn get_val<'a, T: ?Sized + std::str::FromStr>(name: &'a str, args: &'a ArgMatches) -> anyhow::Result<T>
  where <T as FromStr>::Err: std::fmt::Display {
  
    match value_t!(args, name, T) {
    Ok(val) => Ok(val),
    Err(e) => anyhow::bail!("Got an error '{:?}' parsing '{}' from args '{:?}'", e, name, args),
  }
}

pub fn get_val_opt<'a, T: ?Sized + std::str::FromStr>(name: &'a str, args: &'a ArgMatches) -> Option<T>
  where <T as FromStr>::Err: std::fmt::Display {

  match value_t!(args, name, T) {
    Ok(val) => Some(val),
    Err(_) => None,
  }
}

pub fn get_val_mult(name: &str, args: &ArgMatches) -> Vec<String> {
  let mut ret = Vec::new();
  if let Some(in_v) = args.values_of(name) {
    for val in in_v {
      ret.push(val.to_string());
    }
  }
  ret
}

pub fn args_to_json(args: &Vec<String>) -> String {
  let mut ret = String::from("{");
  for arg in args {
    if ! arg.contains("=") {
      panic!("Function argument '{}' does not contain an =", arg);
    }
    let split: Vec<&str> = arg.split("=").collect();
    if split.len() != 2 {
      panic!("Got unexpected number of items ({}) in argument '{}'; Should only have 2", split.len(), arg);
    }
    let fmt = format!("\"{}\":\"{}\"", split[0].to_string(), split[1].to_string());
    if ret.len() > 1 {
      ret.push(',');
    }
    ret.push_str(fmt.as_str());
  }
  ret.push('}');
  ret
}