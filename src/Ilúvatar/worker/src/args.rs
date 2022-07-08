use clap::{ArgMatches, App, SubCommand};

pub fn parse() -> ArgMatches<'static> {
  App::new("ilúvatar_worker")
    .version("0.1.0")
    .about("Ilúvatar worker")
    // TODO: use tihs file
    .args_from_usage(
        "-c, --config=[FILE] 'Sets a custom config file'")
       
    .subcommand(SubCommand::with_name("clean")
                .about("Clean up the system from possible previous executions"))
    .get_matches()
}