pub mod config;
pub mod scaling;
pub mod utils;
#[path ="./trace/trace.rs"]
pub mod trace;
pub mod benchmark;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  let app = config::app();

  let app = trace::trace_args(app);
  let app = scaling::trace_args(app);
  let app = benchmark::trace_args(app);
  let args = app.get_matches();

  match args.subcommand() {
    ("scaling", Some(sub_args)) => { scaling::scaling(&args, &sub_args) },
    ("trace", Some(sub_args)) => { trace::run_trace(&args, &sub_args) },
    ("benchmark", Some(sub_args)) => { benchmark::benchmark_functions(&args, &sub_args) },
    (text,_) => { panic!("Unsupported command {}", text) },
  }?;
  Ok(())
}
