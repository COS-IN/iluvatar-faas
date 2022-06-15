extern crate clap;

use clap::{ArgMatches, App, SubCommand, Arg, value_t};

pub fn parse() -> ArgMatches<'static> {
  App::new("myapp")
    .version("0.1.0")
    .about("Interacts with Ilúvatar workers")
    .args_from_usage(
        "-c, --config=[FILE] 'Sets a custom config file'
        -w, --worker=[NAME]           'Name of worker to send request to'
        -v...                'Sets the level of verbosity'")
    .subcommand(SubCommand::with_name("ping")
                .about("Pings a worker to check if it is up"))
                
    .subcommand(SubCommand::with_name("invoke")
                .about("Invoke a function")
                .arg(Arg::with_name("memory")
                  .short("m")
                  .long("memory")
                  .help("Memory limit of the function")
                  .required(false)
                  .default_value("128")
                  .takes_value(true))
                .arg(Arg::with_name("name")
                  .short("n")
                  .long("name")
                  .help("Name of function to invoke")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("version")
                  .long("version")
                  .default_value("0.1.0")
                  .help("Version of function to invoke")
                  .required(false)
                  .takes_value(true)))

    .subcommand(SubCommand::with_name("invoke-async")
                .about("Invoke a function asynchronously")
                .arg(Arg::with_name("name")
                  .short("n")
                  .long("name")
                  .help("Name of function to invoke")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("arguments")
                  .short("a")
                  .long("arguments")
                  .help("Function arguments")
                  .required(true)
                  .multiple(true)
                  .takes_value(true))
                .arg(Arg::with_name("memory")
                  .short("m")
                  .long("memory")
                  .help("Memory limit of the function")
                  .required(false)
                  .default_value("0")
                  .takes_value(true))
                .arg(Arg::with_name("version")
                  .long("version")
                  .default_value("0.1.0")
                  .help("Version of function to invoke")
                  .required(false)
                  .takes_value(true)))

    .subcommand(SubCommand::with_name("invoke-async-check")
                .about("Check on the status of an asynchronously invoked function")
                .arg(Arg::with_name("cookie")
                  .short("c")
                  .long("cookie")
                  .help("Cookie for async invoke to check")
                  .required(true)
                  .takes_value(true)))
                    
    .subcommand(SubCommand::with_name("prewarm")
                .about("Prewarm a function")
                .arg(Arg::with_name("name")
                  .short("n")
                  .long("name")
                  .help("Name of function to invoke")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("image")
                  .short("i")
                  .long("image")
                  .help("Fully qualified image name for function")
                  .required(false)
                  .default_value("")
                  .takes_value(true))
                .arg(Arg::with_name("memory")
                  .short("m")
                  .long("memory")
                  .help("Memory limit of the function")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("cpu")
                  .short("c")
                  .long("cpu")
                  .help("Number of CPUs to allocate to function")
                  .required(false)
                  .default_value("0")
                  .takes_value(true))
                .arg(Arg::with_name("version")
                  .long("version")
                  .default_value("0.1.0")
                  .help("Version of function to invoke")
                  .required(false)
                  .takes_value(true)))
      
    .subcommand(SubCommand::with_name("register")
                .about("Register a new function")
                .arg(Arg::with_name("name")
                  .short("n")
                  .long("name")
                  .help("Name of function to register")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("image")
                  .short("i")
                  .long("image")
                  .help("Fully qualified image name for function")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("memory")
                  .short("m")
                  .long("memory")
                  .help("Memory limit of the function")
                  .required(true)
                  .takes_value(true))
                .arg(Arg::with_name("cpu")
                  .short("c")
                  .long("cpu")
                  .help("Number of CPUs to allocate to function")
                  .required(false)
                  .default_value("1")
                  .takes_value(true))
                .arg(Arg::with_name("parallel-invokes")
                  .short("p")
                  .long("parallel-invokes")
                  .help("Number of parallel invocations allowed inside one sandbox")
                  .required(false)
                  .default_value("1")
                  .takes_value(true))
                .arg(Arg::with_name("version")
                  .long("version")
                  .default_value("0.1.0")
                  .help("Version of function to register")
                  .required(false)
                  .takes_value(true)))

    .subcommand(SubCommand::with_name("status")
                .about("Get the current status"))
    .subcommand(SubCommand::with_name("health")
                .about("Get the current health status"))
    .get_matches()
}

pub fn get_val<'a, T: ?Sized + std::str::FromStr>(name: &'a str, args: &'a ArgMatches) -> T {
  match value_t!(args, name, T) {
    Ok(val) => val,
    Err(e) => panic!("Got an error '{:?}' parsing '{}' from args '{:?}'", e, name, args),
  }
}

pub fn get_val_opt<'a, T: ?Sized + std::str::FromStr>(name: &'a str, args: &'a ArgMatches) -> Option<T> {
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

pub fn args_to_json(args: Vec<String>) -> String {
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
