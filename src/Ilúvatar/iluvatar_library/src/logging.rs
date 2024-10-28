use crate::bail_error;
use crate::energy::energy_layer::EnergyLayer;
use crate::transaction::TransactionId;
use crate::utils::file_utils::ensure_dir;
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use tracing_flame::FlameLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::SubscriberExt;
use crate::clock::{ClockWrapper, LocalTime, SimulatedTime};

#[derive(Debug, serde::Deserialize, Default)]
/// Details about how/where to log to
pub struct LoggingConfig {
    /// the min log level
    /// see [tracing_subscriber::filter::Builder::parse()]
    pub level: String,
    /// Directory to store logs in, formatted as JSON.
    pub directory: String,
    /// Additionally write logs to stdout.
    pub stdout: Option<bool>,
    /// log filename start string
    pub basename: String,
    /// How to log spans, in all caps
    /// look at for details [mod@tracing_subscriber::fmt::format]
    /// Multiple options can be passed by listing them as a list using '+' between values.
    /// The recommended value is `NEW+CLOSE`, which creates logs on span (function) execution and completion, allowing tracking of when a function call starts and stops.
    /// More expressive span capturing naturally means larger log files.
    pub spanning: String,
    /// A file name to put flame trace data in, file will be placed in [Self::directory] if present, or local directory.
    /// See (here)<https://docs.rs/tracing-flame/latest/tracing_flame/index.html> for details on how this works.
    /// If [None], do not record flame data.
    pub flame: Option<String>,
    /// Use live span information to track invocation and background energy usage.
    /// These will then be reported to influx.
    pub span_energy_monitoring: bool,
}

#[allow(dead_code)]
fn str_to_span(spanning: &str) -> Result<FmtSpan> {
    let mut fmt = FmtSpan::NONE;
    for choice in spanning.split('+') {
        fmt |= match choice {
            "NEW" => FmtSpan::NEW,
            "ENTER" => FmtSpan::ENTER,
            "EXIT" => FmtSpan::EXIT,
            "CLOSE" => FmtSpan::CLOSE,
            "NONE" => FmtSpan::NONE,
            "" => FmtSpan::NONE,
            "ACTIVE" => FmtSpan::ACTIVE,
            "FULL" => FmtSpan::FULL,
            _ => anyhow::bail!("Unknown spanning value {}", choice),
        }
    }
    Ok(fmt)
}

fn __start_tracing<T: FormatTime + Send + Sync + 'static>(config: Arc<LoggingConfig>, worker_name: &str, tid: &TransactionId, clock: T) -> Result<impl Drop> {
    let fname = format!("{}.log", config.basename.clone());
    let buff = PathBuf::new();
    ensure_dir(&buff.join(&config.directory))?;
    let dir = match std::fs::canonicalize(config.directory.clone()) {
        Ok(d) => d,
        Err(e) => anyhow::bail!("Failed to canonicalize log file '{}'", e),
    };
    ensure_dir(&dir)?;

    let full_path = std::path::Path::new(&dir).join(&fname);
    println!("Logging to {}", full_path.to_string_lossy());
    if full_path.exists() {
        match std::fs::remove_file(full_path) {
            Ok(_) => (),
            Err(e) => anyhow::bail!("Failed to remove old log file because '{}'", e),
        };
    }

    let appender = tracing_appender::rolling::never(dir, fname);
    let (file_writer, guard) = tracing_appender::non_blocking(appender);
    #[allow(dyn_drop)]
    let mut drops: Vec<Box<dyn Drop>> = vec![Box::new(guard)];
    let stdout_layer = match config.stdout.unwrap_or(false) {
        true => {
            let (stdout, guard) = tracing_appender::non_blocking(std::io::stdout());
            drops.push(Box::new(guard));
            Some(tracing_subscriber::fmt::Layer::default().with_writer(stdout).compact())
        }
        false => None,
    };

    let energy_layer = match config.span_energy_monitoring {
        true => Some(EnergyLayer::new(worker_name)),
        false => None,
    };

    let flame_layer = match config.flame.as_ref() {
        Some(flame) if !flame.is_empty() => {
            let flame_path = PathBuf::from(&config.directory).join(flame);
            let (mut flame_layer, _flame_guard) = match FlameLayer::with_file(flame_path) {
                Ok(l) => l,
                Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make FlameLayer"),
            };
            flame_layer = flame_layer.with_threads_collapsed(true).with_file_and_line(true);
            drops.push(Box::new(_flame_guard));
            Some(flame_layer)
        }
        _ => None,
    };

    // This is ugly, but there isn't an alternative
    // https://github.com/tokio-rs/tracing/issues/1846
    // Bad trait bound on `with_timer` causes compile failure if using Arc<dyn FormatTime>
    let subscriber = tracing_subscriber::fmt()
        .with_timer(clock)
        .with_span_events(str_to_span(&config.spanning)?)
        .with_env_filter(EnvFilter::builder().parse(&config.level)?)
        .with_writer(file_writer)
        .json()
        .finish();

    let subscriber = subscriber.with(stdout_layer).with(energy_layer).with(flame_layer);
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(drops)
}

pub fn start_tracing(config: Arc<LoggingConfig>, worker_name: &str, tid: &TransactionId) -> Result<impl Drop> {
    // This is ugly, but there isn't an alternative
    // https://github.com/tokio-rs/tracing/issues/1846
    // Bad trait bound on `with_timer` causes compile failure if using Arc<dyn FormatTime>
    let r = match crate::utils::is_simulation() {
        true => __start_tracing(config, worker_name, tid, ClockWrapper(LocalTime::boxed(tid)?)),
        false => __start_tracing(config, worker_name, tid, ClockWrapper(SimulatedTime::boxed(tid)?)),
    };
    r
   //  let fname = format!("{}.log", config.basename.clone());
   //  let buff = PathBuf::new();
   //  ensure_dir(&buff.join(&config.directory))?;
   //  let dir = match std::fs::canonicalize(config.directory.clone()) {
   //      Ok(d) => d,
   //      Err(e) => anyhow::bail!("Failed to canonicalize log file '{}'", e),
   //  };
   //  ensure_dir(&dir)?;
   // 
   //  let full_path = std::path::Path::new(&dir).join(&fname);
   //  println!("Logging to {}", full_path.to_string_lossy());
   //  if full_path.exists() {
   //      match std::fs::remove_file(full_path) {
   //          Ok(_) => (),
   //          Err(e) => anyhow::bail!("Failed to remove old log file because '{}'", e),
   //      };
   //  }
   // 
   //  let appender = tracing_appender::rolling::never(dir, fname);
   //  let (file_writer, guard) = tracing_appender::non_blocking(appender);
   //  #[allow(dyn_drop)]
   //  let mut drops: Vec<Box<dyn Drop>> = vec![Box::new(guard)];
   //  let stdout_layer = match config.stdout.unwrap_or(false) {
   //      true => {
   //          let (stdout, guard) = tracing_appender::non_blocking(std::io::stdout());
   //          drops.push(Box::new(guard));
   //          Some(tracing_subscriber::fmt::Layer::default().with_writer(stdout).compact())
   //      }
   //      false => None,
   //  };
   // 
   //  let energy_layer = match config.span_energy_monitoring {
   //      true => Some(EnergyLayer::new(worker_name)),
   //      false => None,
   //  };
   // 
   //  let flame_layer = match config.flame.as_ref() {
   //      Some(flame) if !flame.is_empty() => {
   //          let flame_path = PathBuf::from(&config.directory).join(flame);
   //          let (mut flame_layer, _flame_guard) = match FlameLayer::with_file(flame_path) {
   //              Ok(l) => l,
   //              Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make FlameLayer"),
   //          };
   //          flame_layer = flame_layer.with_threads_collapsed(true).with_file_and_line(true);
   //          drops.push(Box::new(_flame_guard));
   //          Some(flame_layer)
   //      }
   //      _ => None,
   //  };
   // 
   //  // This is ugly, but there isn't an alternative
   //  // https://github.com/tokio-rs/tracing/issues/1846
   //  // Bad trait bound on `with_timer` causes compile failure if using Arc<dyn FormatTime>
   //  // match crate::utils::is_simulation() {
   //  //     true => {
   //  //         let writer_layer = tracing_subscriber::fmt::layer()
   //  //             .with_span_events(str_to_span(&config.spanning))
   //  //             .with_timer(SimulatedTime::new(tid)?)
   //  //             .with_writer(non_blocking);
   //  //         match config.directory {
   //  //             None => layers.with(writer_layer).init(),
   //  //             Some(_) => layers.with(writer_layer.json()).init(),
   //  //         };
   //  //     },
   //  //     false => {
   //  //         let writer_layer = tracing_subscriber::fmt::layer()
   //  //             .with_span_events(str_to_span(&config.spanning))
   //  //             .with_timer(LocalTime::new(tid)?)
   //  //             .with_writer(non_blocking);
   //  //         match config.directory {
   //  //             None => layers.with(writer_layer).init(),
   //  //             Some(_) => layers.with(writer_layer.json()).init(),
   //  //         };
   //  //     }
   //  // }
   // 
   //  // This is ugly, but there isn't an alternative
   //  // https://github.com/tokio-rs/tracing/issues/1846
   //  // Bad trait bound on `with_timer` causes compile failure if using Arc<dyn FormatTime>
   //  match crate::utils::is_simulation() {
   //      true => {
   //          let subscriber = tracing_subscriber::fmt()
   //              .with_timer(SimulatedTime::new(tid)?)
   //              .with_span_events(str_to_span(&config.spanning)?)
   //              .with_env_filter(EnvFilter::builder().parse(&config.level)?)
   //              .with_writer(file_writer)
   //              .json()
   //              .finish();
   // 
   //          let subscriber = subscriber.with(stdout_layer).with(energy_layer).with(flame_layer);
   //          tracing::subscriber::set_global_default(subscriber)?;
   //      },
   //      false => {
   //          // let subscriber = tracing_subscriber::fmt()
   //          //     .with_timer(LocalTime::new(tid)?)
   //          //     .with_span_events(str_to_span(&config.spanning)?)
   //          //     .with_env_filter(EnvFilter::builder().parse(&config.level)?)
   //          //     .with_writer(file_writer)
   //          //     .json()
   //          //     .finish();
   //          // 
   //          // let subscriber = subscriber.with(stdout_layer).with(energy_layer).with(flame_layer);
   //          // tracing::subscriber::set_global_default(subscriber)?;
   //          // Arc::new(LocalTime::new(tid)?) 
   //      },
   //  };
   // Ok(drops)
}
