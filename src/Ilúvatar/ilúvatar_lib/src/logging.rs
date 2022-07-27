use std::{sync::Arc, path::PathBuf};
use tracing::metadata::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;
use anyhow::Result;

use crate::utils::file_utils::ensure_dir;

#[derive(Debug, serde::Deserialize)]
#[allow(unused)]
/// details about how/where to log to
pub struct LoggingConfig {
  /// the min log level
  /// see [tracing_core::metadata::LevelFilter]
  pub level: String,
  /// directory to store logs in
  /// logs to stdout if empty
  pub directory: String,
  /// log filename start string
  pub basename: String,
  /// how to log spans 
  /// look at for details [tracing_subscriber::fmt::format]
  pub spanning: String,
}

fn str_to_span(spanning: &String) -> FmtSpan {
  match spanning.to_lowercase().as_str() {
    "new" => FmtSpan::NEW,
    "enter" => FmtSpan::ENTER,
    "exit" => FmtSpan::EXIT,
    "close" => FmtSpan::CLOSE,
    "none" => FmtSpan::NONE,
    "active" => FmtSpan::ACTIVE,
    "full" => FmtSpan::FULL,
    _ => panic!("Unknown spanning value {}", spanning),
  }
}

pub fn start_tracing(config: Arc<LoggingConfig>) -> Result<WorkerGuard> {
  let (non_blocking, _guard) = match config.directory.as_str() {
    "" => tracing_appender::non_blocking(std::io::stdout()),
    _ => {
      let fname = format!("{}.log", config.basename.clone());
      let buff = PathBuf::new();
      ensure_dir(&buff.join(&config.directory))?;
      let dir = std::fs::canonicalize(config.directory.clone())?;
      ensure_dir(&dir)?;

      let full_path = std::path::Path::new(&dir).join(&fname);
      println!("Logging to {}", full_path.to_str().unwrap());
      if full_path.exists() {
        std::fs::remove_file(full_path).unwrap();
      }

      let appender = tracing_appender::rolling::never(dir, fname);
      tracing_appender::non_blocking(appender)
    }
  };

  let builder = tracing_subscriber::fmt()
    .with_max_level(config.level.parse::<LevelFilter>()?)
    .with_span_events(str_to_span(&config.spanning))
    .with_writer(non_blocking);

  match config.directory.as_str() {
    "" => builder.init(),
    _ => builder.json().with_span_list(false).init(),
  };
  
  Ok(_guard)
}
