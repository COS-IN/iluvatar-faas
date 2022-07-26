use scaling::scaling;

pub mod config;
pub mod scaling;
pub mod utils;

fn main() -> Result<(), Box<dyn std::error::Error>> {

  let args = config::parse();

  match args.subcommand() {
    ("scaling", Some(sub_args)) => { scaling(&args, &sub_args) },
    (text,_) => { panic!("Unsupported command {}", text) },
  }?;
  Ok(())
}
