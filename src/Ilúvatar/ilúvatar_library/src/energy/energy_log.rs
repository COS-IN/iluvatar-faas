use std::{path::Path, fs::File, io::Write};
use anyhow::Result;
use crate::transaction::TransactionId;
use super::{perf::start_perf_stat, rapl::RAPL};

pub fn log_energy_usage(out_folder: &str, tid: &TransactionId, stat_duration_sec: u64) -> Result<()> {
  let perf_file = Path::new(out_folder);
  let perf_file = perf_file.join("perf.log").as_path().to_str().unwrap().to_string();
  let _child = start_perf_stat(&perf_file, tid, stat_duration_sec)?;

  monitor_rapl(out_folder, stat_duration_sec)?;

  Ok(())
}

fn monitor_rapl(out_folder: &str, stat_duration_sec: u64) -> Result<()> {
  let p = Path::new(&out_folder).join(format!("rapl.log"));
  let tid: &TransactionId = &crate::transaction::ENERGY_MONITOR_TID;
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  match f.write_all("elapsed_ns,uj\n".as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      println!("Failed to write header of result because {}", e);
    }
  };
  let rapl = RAPL::new()?;

  let mut curr_rapl = rapl.record()?;
  let mut elapsed = 0;
  loop {
    let new_rapl = rapl.record()?;
    let (time, uj) = rapl.difference(&new_rapl, &curr_rapl, tid)?;
    elapsed += time;
    let to_write = format!("{},{}\n", elapsed, uj);
    match f.write_all(to_write.as_bytes()) {
      Ok(_) => (),
      Err(e) => {
        println!("Failed to write csv of result because {}", e);
      }
    };
    curr_rapl = new_rapl;

    std::thread::sleep(std::time::Duration::from_secs(stat_duration_sec));
  }
}
