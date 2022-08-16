use std::collections::HashMap;

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
    let (function_data, overhead) = monitor.read_log()?;
    if function_data.len() > 0 {
      let tot_time = (function_data.values().sum::<i128>() + overhead) as f64;
      println!("Overhead: {}; Total time: {}; Overhead share: {}", overhead, tot_time, overhead as f64 / tot_time);
      let mut shares = HashMap::new();
      for (k,v) in function_data.iter() {
        let share = *v as f64 / tot_time;
        shares.insert(k.clone(), share);
      }
      println!("{:?}", shares);
  
      let rapl = rapl::RAPL::record()?;
      let (time, uj) = rapl.difference(&curr_rapl)?;
      println!("{} seconds; {} joules;", time / 1000000, uj / 1000000);
      curr_rapl= rapl;
    }

    std::thread::sleep(std::time::Duration::from_secs(1));
  }
}
