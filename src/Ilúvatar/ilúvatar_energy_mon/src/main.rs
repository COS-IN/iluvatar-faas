use iluvatar_library::utils::config::get_val;
use read::LogMonitor;

pub mod config;
pub mod read;
pub mod structs;
pub mod rapl;

fn main() -> anyhow::Result<()> {
  let args = config::parse();
  let log_file: String = get_val("file", &args)?;
  let mut monitor = LogMonitor::new(&log_file)?;
  let mut curr_rapl = rapl::RAPL::record()?;

  loop {
    monitor.read_log()?;
  
    println!("{:?}", monitor.functions);

    let rapl = rapl::RAPL::record()?;
    let (time, uj) = rapl.difference(&curr_rapl)?;
    println!("{} seconds; {} joules;", time / 1000000, uj / 1000000);
    curr_rapl= rapl;
    std::thread::sleep(std::time::Duration::from_secs(1));
  }
}
