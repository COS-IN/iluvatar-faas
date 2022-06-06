extern crate clap;
use clap::{ArgMatches, App, SubCommand, Arg};

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
                .arg(Arg::with_name("version")
                  .long("version")
                  .default_value("0.1.0")
                  .help("Version of function to invoke")
                  .required(false)
                  .takes_value(true)))
    .subcommand(SubCommand::with_name("prewarm")
                .about("Prewarm a function")
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
      
    .subcommand(SubCommand::with_name("register")
                .about("Register a new function")
                .arg(Arg::with_name("name")
                  .short("n")
                  .long("name")
                  .help("Name of function to register")
                  .required(true)
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

pub fn get_val<'a>(name: &'a str, args: &'a ArgMatches) -> &'a str {
  if let Some(val) = args.value_of(name) {
    return val;
  }
  panic!("Unable to find '{}' in args '{:?}'", name, args)
}
