extern crate clap;

use clap::{ArgMatches, App, SubCommand, Arg};

pub fn parse() -> ArgMatches<'static> {
  App::new("myapp")
    .version("0.1.0")
    .about("Interacts with Ilúvatar workers")
    .args_from_usage("
        -p, --port=[PORT]           'Port worker is listening on'
        -h, --host=[NAME]           'Host worker is on'
        -o, --out=[FOLDER]          'Folder to output results to'
        -i, --iterations=[ITERS]    'Number of times to run experiment'")
    .subcommand(SubCommand::with_name("scaling")
                .about("Test scaling of worker with increasing amount of requests")
                .arg(Arg::with_name("start")
                  .short("s")
                  .long("start")
                  .help("Number of threads to start")
                  .required(false)
                  .takes_value(true)
                  .default_value("1"))
                .arg(Arg::with_name("end")
                  .short("e")
                  .long("end")
                  .help("Number of threads to reach")
                  .required(false)
                  .takes_value(true)
                  .default_value("1"))
                .arg(Arg::with_name("duration")
                  .short("d")
                  .long("duration")
                  .help("Duration in seconds before increasing load")
                  .required(false)
                  .takes_value(true)
                  .default_value("5"))
                )
    .get_matches()
}
