use std::{sync::Arc, path::PathBuf};
use tracing::metadata::LevelFilter;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use anyhow::Result;
use tracing_flame::FlameLayer;
use tracing_subscriber::prelude::*;
use crate::{utils::file_utils::ensure_dir, energy::EnergyLayer};

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
  /// How to log spans, in all caps
  /// look at for details [tracing_subscriber::fmt::format]
  /// Multiple options can be passed by listing them as a list using '+' between values
  pub spanning: String,
  /// a file to put flame trace data in 
  /// See (here)[https://docs.rs/tracing-flame/latest/tracing_flame/index.html] for details
  /// If empty, do not record flame data
  /// Enabling this loses some logging information
  pub flame: String,
}

fn str_to_span(spanning: &String) -> FmtSpan {
  let mut fmt = FmtSpan::NONE;
  for choice in spanning.split("+") {
    fmt |= match choice {
      "NEW" => FmtSpan::NEW,
      "ENTER" => FmtSpan::ENTER,
      "EXIT" => FmtSpan::EXIT,
      "CLOSE" => FmtSpan::CLOSE,
      "NONE" => FmtSpan::NONE,
      "ACTIVE" => FmtSpan::ACTIVE,
      "FULL" => FmtSpan::FULL,
      _ => panic!("Unknown spanning value {}", choice),
    }
  }
  fmt
}

pub fn start_tracing(config: Arc<LoggingConfig>) -> Result<impl Drop> {
  #[allow(dyn_drop)]
  let mut drops:Vec<Box<dyn Drop>> = Vec::new();
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
  drops.push(Box::new(_guard));

  match config.flame.as_str() {
    "" => {
      // tracing_subscriber:;registry()
      //   .with(EnergyLayer)
      //   .

      let builder = tracing_subscriber::fmt()
        .with_max_level(config.level.parse::<LevelFilter>()?)
        .with_span_events(str_to_span(&config.spanning))
        .with_writer(non_blocking);
      match config.directory.as_str() {
        "" => builder.init(),
        _ => builder.json().with_span_list(true).init(),
      };
    },
    _ => {
      let filter_layer = EnvFilter::builder()
                          .parse(&config.level)?;
      let (flame_layer, _flame_guard) = FlameLayer::with_file(&config.flame).unwrap();
      tracing_subscriber::registry()
        .with(filter_layer)
        .with(flame_layer)
        .with(EnergyLayer {})
        .init();
      drops.push(Box::new(_flame_guard));
    }
  };
 
  Ok(drops)
}
