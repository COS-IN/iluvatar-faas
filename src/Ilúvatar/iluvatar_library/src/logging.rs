use crate::bail_error;
use crate::energy::energy_layer::EnergyLayer;
use crate::transaction::TransactionId;
use crate::utils::file_utils::ensure_dir;
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use tracing_flame::FlameLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{prelude::*, Registry};
use crate::clock::{LocalTime, SimulatedTime};

#[derive(Debug, serde::Deserialize)]
/// Details about how/where to log to
pub struct LoggingConfig {
    /// the min log level
    /// see [tracing_subscriber::filter::Builder::parse()]
    pub level: String,
    /// Directory to store logs in, formatted as JSON
    /// If [None] then logs are sent to stdout, in a plaintext format
    pub directory: Option<String>,
    /// log filename start string
    pub basename: String,
    /// How to log spans, in all caps
    /// look at for details [mod@tracing_subscriber::fmt::format]
    /// Multiple options can be passed by listing them as a list using '+' between values.
    /// The recommended value is `NEW+CLOSE`, which creates logs on span (function) execution and completion, allowing tracking of when a function call starts and stops.
    /// More expressive span capturing naturally means larger log files.
    pub spanning: String,
    /// A file name to put flame trace data in, file will be placed in [Self::directory] if present, or local directory
    /// See (here)<https://docs.rs/tracing-flame/latest/tracing_flame/index.html> for details on how this works.
    /// If [None], do not record flame data
    pub flame: Option<String>,
    /// Use live span information to track invocation and background energy usage
    /// These will then be reported to influx
    pub span_energy_monitoring: bool,
}

fn str_to_span(spanning: &str) -> FmtSpan {
    let mut fmt = FmtSpan::NONE;
    for choice in spanning.split('+') {
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

pub fn start_tracing(config: Arc<LoggingConfig>, worker_name: &str, tid: &TransactionId) -> Result<impl Drop> {
    #[allow(dyn_drop)]
    let mut drops: Vec<Box<dyn Drop>> = Vec::new();
    let (non_blocking, _guard) = match config.directory.as_ref() {
        None => tracing_appender::non_blocking(std::io::stdout()),
        Some(log_dir) => {
            let fname = format!("{}.log", config.basename.clone());
            let buff = PathBuf::new();
            ensure_dir(&buff.join(log_dir))?;
            let dir = match std::fs::canonicalize(log_dir.clone()) {
                Ok(d) => d,
                Err(e) => anyhow::bail!("Failed to remove canonicalize log file '{}'", e),
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
            tracing_appender::non_blocking(appender)
        }
    };
    drops.push(Box::new(_guard));

    let energy_layer = match config.span_energy_monitoring {
        true => Some(EnergyLayer::new(worker_name)),
        false => None,
    };
    let filter_layer = EnvFilter::builder().parse(&config.level)?;
    let layers = Registry::default().with(filter_layer).with(energy_layer);

    let layers = match config.flame.as_ref() {
        None => layers.with(None),
        Some(flame) => {
            if flame.is_empty() {
                layers.with(None)
            } else {
                let flame_path = match config.directory.as_ref() {
                    Some(p) => PathBuf::from(p).join(flame),
                    None => PathBuf::from(flame),
                };
                let (mut flame_layer, _flame_guard) = match FlameLayer::with_file(flame_path) {
                    Ok(l) => l,
                    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make FlameLayer"),
                };
                flame_layer = flame_layer.with_threads_collapsed(true).with_file_and_line(true);
                drops.push(Box::new(_flame_guard));
                layers.with(Some(flame_layer))
            }
        }
    };

    // This is ugly, but there isn't an alternative
    // https://github.com/tokio-rs/tracing/issues/1846
    // Bad trait bound on `with_timer` causes compile failure if using Arc<dyn FormatTime>
    match crate::utils::is_simulation() {
        true => {
            let writer_layer = tracing_subscriber::fmt::layer()
                .with_span_events(str_to_span(&config.spanning))
                .with_timer(SimulatedTime::new(tid)?)
                .with_writer(non_blocking);
            match config.directory {
                None => layers.with(writer_layer).init(),
                Some(_) => layers.with(writer_layer.json()).init(),
            };
        },
        false => {
            let writer_layer = tracing_subscriber::fmt::layer()
                .with_span_events(str_to_span(&config.spanning))
                .with_timer(LocalTime::new(tid)?)
                .with_writer(non_blocking);
            match config.directory {
                None => layers.with(writer_layer).init(),
                Some(_) => layers.with(writer_layer.json()).init(),
            };   
        }
    }
    Ok(drops)
}
