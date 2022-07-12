use flexi_logger::{DeferredNow, Record, TS_DASHES_BLANK_COLONS_DOT_BLANK, Logger, FileSpec, WriteMode, LoggerHandle};
use iluvatar_lib::transaction::TransactionId;
use log::debug;

use iluvatar_lib::worker_api::config::WorkerConfig;


pub fn timed_format(
  w: &mut dyn std::io::Write,
  now: &mut DeferredNow,
  record: &Record,
) -> Result<(), std::io::Error> {
  write!(
      w,
      "[{}] [{}] [{}] {}",
      now.format(TS_DASHES_BLANK_COLONS_DOT_BLANK),
      record.level(),
      record.module_path().unwrap_or("<unnamed>"),
      &record.args()
  )
}

pub fn make_logger(server_config: &WorkerConfig, tid: &TransactionId, log_mode: WriteMode) -> LoggerHandle {
  debug!("[{}] Creating logger", tid,);
  Logger::try_with_str(server_config.logging.level.as_str()).unwrap()
    .log_to_file(FileSpec::default()
                    .directory(server_config.logging.directory.as_str())
                    .basename(server_config.logging.basename.as_str())
                  )
    .format(timed_format)
    .write_mode(log_mode)
    .create_symlink("iluvatar_worker.log")
    // .create_symlink(iluvatar_lib::utils::file::temp_file("iluvatar_worker", "log").unwrap())
    .print_message()
    .start().unwrap()
}