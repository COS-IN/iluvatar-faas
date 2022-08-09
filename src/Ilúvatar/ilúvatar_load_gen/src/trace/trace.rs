use std::collections::HashMap;
use anyhow::Result;
use iluvatar_lib::{utils::config::get_val, types::MemSizeMb};
use clap::{ArgMatches, App, SubCommand, Arg};

mod worker_trace;
mod controller_trace;

pub fn trace_args<'a>(app: App<'a, 'a>) -> App<'a, 'a> {
  app.subcommand(SubCommand::with_name("trace")
    .about("Run a trace through the system")
    .arg(Arg::with_name("setup")
        .long("setup")
        .help("Use simulation or live system")
        .required(true)
        .takes_value(true)
        .default_value("simulation"))
    .arg(Arg::with_name("target")
        .short("t")
        .long("target")
        .help("Target for the load, either 'worker' or 'controller'")
        .required(false)
        .takes_value(true)
        .default_value("worker"))
    .arg(Arg::with_name("input")
        .short("i")
        .long("input")
        .help("The trace input csv file to use")
        .required(true)
        .takes_value(true))
    .arg(Arg::with_name("metadata")
        .short("m")
        .long("metadata")
        .help("The metadata associated with the trace input, also a csv file")
        .required(true)
        .takes_value(true))
    .arg(Arg::with_name("worker-config")
        .long("worker-config")
        .help("Configuration file for the worker")
        .required(false)
        .takes_value(true))
    .arg(Arg::with_name("controller-config")
        .long("controller-config")
        .help("Configuration file for the controller")
        .required(false)
        .takes_value(true))
    .arg(Arg::with_name("load-type")
        .short("l")
        .long("load-type")
        .help("Type of load to apply, use 'lookbusy' containers or 'functions' for FunctionBench code")
        .required(false)
        .default_value("lookbusy")
        .takes_value(true))
    .arg(Arg::with_name("function-data")
        .short("f")
        .long("function-data")
        .help("If using FunctionBench data, this file is the results of the `benchmark` run. Used to pick which function matches the trace function.")
        .required(false)
        .takes_value(true))
      )
}

pub fn run_trace(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let target: String = get_val("target", &sub_args)?;

  match target.as_str() {
    "worker" => worker_trace::trace_worker(main_args, sub_args),
    "controller" => controller_trace::trace_controller(main_args, sub_args),
    _ => anyhow::bail!("Unknown simulation targe {}!", target),
  }
}

#[derive(Debug, serde::Deserialize)]
#[allow(unused)]
struct Function {
  pub func_name: String,
  pub cold_dur_ms: u64,
  pub warm_dur_ms: u64,
  pub mem_mb: MemSizeMb,
  pub function_id: u64,
}
#[derive(Debug, serde::Deserialize)]
struct CsvInvocation {
  function_id: u64,
  invoke_time_ms: u64,
}

fn load_metadata(path: String) -> Result<HashMap<u64, Function>> {
  let mut rdr = csv::Reader::from_path(path)?;
  let mut ret = HashMap::new();
  for result in rdr.deserialize() {
    let func: Function = result?;
    ret.insert(func.function_id, func);
  }
  Ok(ret)
}
