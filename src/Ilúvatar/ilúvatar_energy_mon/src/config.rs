use clap::{ArgMatches, App, Arg};

pub fn parse() -> ArgMatches<'static> {
  App::new("ilúvatar_energy_mon")
    .version("0.1.0")
    .about("Ilúvatar Energy Monitor")
    .arg(Arg::with_name("file")
      .short("f")
      .long("file")
      .help("Path to a log file to consume")
      .required(true)
      .default_value("/tmp/foo/bar")
      .takes_value(true))
    .get_matches()
}