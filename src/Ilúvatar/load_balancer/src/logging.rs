use std::sync::Arc;
use flexi_logger::{Logger, FileSpec, WriteMode, LoggerHandle};
use iluvatar_lib::transaction::TransactionId;
use iluvatar_lib::logging::timed_format;
use log::debug;

use iluvatar_lib::load_balancer_api::config::LoggingConfig;

pub fn make_logger(config: Arc<LoggingConfig>, tid: &TransactionId, log_mode: WriteMode) -> LoggerHandle {
  debug!("[{}] Creating logger", tid);
  let symlink = if cfg!(debug_assertions) {
    "iluvatar_load_balancer.log".to_string()
  }  else {
    iluvatar_lib::utils::file::temp_file_pth("iluvatar_load_balancer", "log")
  };
  let path = std::fs::canonicalize(config.directory.as_str()).unwrap();
  std::fs::create_dir_all(&path).unwrap();
  Logger::try_with_str(config.level.as_str()).unwrap()
    .log_to_file(FileSpec::default()
                    .directory(path)
                    .basename(config.basename.as_str())
                  )
    .format(timed_format)
    .write_mode(log_mode)
    .create_symlink(symlink)
    .print_message()
    .start().unwrap()
}