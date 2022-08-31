extern crate clap;

use clap::App;

pub fn app() -> App<'static> {
  App::new("myapp")
  .version("0.1.0")
  .about("Interacts with Ilúvatar workers")
  .args_from_usage("
      -p, --port=[PORT]           'Port controller/worker is listening on'
      -h, --host=[NAME]           'Host controller/worker is on'
      -o, --out=[FOLDER]          'Folder to output results to'")
}
