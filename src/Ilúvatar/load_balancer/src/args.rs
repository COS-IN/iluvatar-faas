use clap::{ArgMatches, App, Arg};

pub fn parse() -> ArgMatches<'static> {
  App::new("ilúvatar_controller")
    .version("0.1.0")
    .about("Ilúvatar controller")
    .arg(Arg::with_name("config")
      .short("c")
      .long("config")
      .help("Path to a configuration file to use")
      .required(false)
      .default_value("/tmp/foo/bar")
      .takes_value(true))
    .get_matches()
}