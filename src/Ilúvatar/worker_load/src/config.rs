extern crate clap;

use clap::App;

pub fn app() -> App<'static, 'static> {
  App::new("myapp")
  .version("0.1.0")
  .about("Interacts with Ilúvatar workers")
  .args_from_usage("
      -p, --port=[PORT]           'Port worker is listening on'
      -h, --host=[NAME]           'Host worker is on'
      -o, --out=[FOLDER]          'Folder to output results to'
      -i, --iterations=[ITERS]    'Number of times to run experiment'")
}
