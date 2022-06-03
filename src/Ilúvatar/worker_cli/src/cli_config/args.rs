extern crate clap;
use clap::{ArgMatches, App, SubCommand};

pub fn parse() -> ArgMatches<'static> {
  App::new("myapp")
    .version("0.1.0")
    .about("Interacts with Ilúvatar workers")
    .args_from_usage(
        "-c, --config=[FILE] 'Sets a custom config file'
        -n, --name=[NAME]           'Name of worker to send request to'
        -v...                'Sets the level of verbosity'")
    .subcommand(SubCommand::with_name("ping")
                .about("Pings a worker to check if it is up"))
    .subcommand(SubCommand::with_name("invoke")
                .about("Invoke a function"))
    .subcommand(SubCommand::with_name("register")
                .about("Register a new function"))
    .subcommand(SubCommand::with_name("status")
                .about("Get the current status"))
    .get_matches()
}