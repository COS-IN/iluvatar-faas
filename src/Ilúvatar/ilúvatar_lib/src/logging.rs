use std::sync::Arc;
use tracing::metadata::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;
use anyhow::Result;

#[derive(Debug, serde::Deserialize)]
#[allow(unused)]
/// details about how/where to log to
pub struct LoggingConfig {
  /// the min log level
  pub level: String,
  /// directory to store logs in
  /// logs to stdout if empty
  pub directory: String,
  /// log filename start string
  pub basename: String
}

pub fn start_tracing(config: Arc<LoggingConfig>) -> Result<WorkerGuard> {
  let (non_blocking, _guard) = match config.directory.as_str() {
    "" => tracing_appender::non_blocking(std::io::stdout()),
    _ => {
      let fname = format!("{}.log", config.basename.clone());
      let dir = std::fs::canonicalize(config.directory.clone())?;
      println!("Logging to {}/{}", dir.to_str().unwrap(), fname);
      let appender = tracing_appender::rolling::never(dir, fname);
      tracing_appender::non_blocking(appender)
    }
  };

  let builder = tracing_subscriber::fmt()
    .with_max_level(config.level.parse::<LevelFilter>()?)
    .with_span_events(FmtSpan::FULL)
    .with_writer(non_blocking);

  match config.directory.as_str() {
    "" => builder.init(),
    _ => builder.json().init(),
  };
  
  Ok(_guard)
}
