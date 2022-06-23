use flexi_logger::{DeferredNow, Record, TS_DASHES_BLANK_COLONS_DOT_BLANK, Logger, FileSpec, WriteMode, LoggerHandle};

use crate::config::WorkerConfig;


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

pub fn make_logger(server_config: &WorkerConfig) -> LoggerHandle {
  Logger::try_with_str(server_config.logging.level.as_str()).unwrap()
    .log_to_file(FileSpec::default()
                    .directory(server_config.logging.directory.as_str())
                    .basename(server_config.logging.basename.as_str())
                  )
    .format(timed_format)
    .write_mode(WriteMode::Async)
    .create_symlink("iluvitar_worker.log")
    .create_symlink(iluvatar_lib::utils::temp_file("iluvitar_worker", "log").unwrap())
    .print_message()
    .start().unwrap()
}