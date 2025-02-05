use crate::bail_error;
use crate::clock::{get_global_clock, ClockWrapper};
use crate::energy::energy_layer::EnergyLayer;
use crate::transaction::TransactionId;
use crate::utils::file_utils::ensure_dir;
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use tracing::{info, warn};
use tracing_flame::FlameLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::Registry;

#[derive(Debug, serde::Deserialize, Default, Clone)]
/// Details about how/where to log to
pub struct LoggingConfig {
    /// the min log level
    /// see [tracing_subscriber::filter::Builder::parse()]
    pub level: String,
    /// Directory to store logs in, formatted as JSON.
    pub directory: String,
    /// Additionally write logs to stdout.
    #[serde(default)]
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
    #[serde(default)]
    pub flame: Option<String>,
    /// Use live span information to track invocation and background energy usage.
    /// These will then be reported to influx.
    #[serde(default)]
    pub span_energy_monitoring: bool,
    /// Include currently entered spans when logging JSON messages.
    /// See (here for more details)<https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/format/struct.Json.html#method.with_span_list>
    #[serde(default)]
    pub include_spans_json: bool,
}

fn parse_span(span: &str) -> Result<FmtSpan> {
    Ok(match span {
        "NEW" => FmtSpan::NEW,
        "ENTER" => FmtSpan::ENTER,
        "EXIT" => FmtSpan::EXIT,
        "CLOSE" => FmtSpan::CLOSE,
        "NONE" => FmtSpan::NONE,
        "" => FmtSpan::NONE,
        "ACTIVE" => FmtSpan::ACTIVE,
        "FULL" => FmtSpan::FULL,
        _ => anyhow::bail!("Unknown spanning value {}", span),
    })
}
fn str_to_span(spanning: &str) -> Result<FmtSpan> {
    let parts = spanning.split('+').collect::<Vec<&str>>();
    if parts.is_empty() {
        return Ok(FmtSpan::NONE);
    }
    let mut parts = parts
        .iter()
        .map(|span| parse_span(span))
        .collect::<Result<Vec<FmtSpan>>>()?;
    let first_part = parts.pop().unwrap();
    Ok(parts.into_iter().fold(first_part, |acc, item| item | acc))
}

fn panic_hook() {
    std::panic::set_hook(Box::new(move |info| {
        println!("!!Thread panicked!!");
        let backtrace = std::backtrace::Backtrace::force_capture();
        let thread = std::thread::current();
        let thread = thread.name().unwrap_or("<unnamed>");

        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            },
        };

        match info.location() {
            Some(location) => {
                tracing::error!(
                    target: "panic", "thread '{}' panicked at '{}': {}:{}{:?}",
                    thread,
                    msg,
                    location.file(),
                    location.line(),
                    backtrace
                );
            },
            None => tracing::error!(
                target: "panic",
                "thread '{}' panicked at '{}'{:?}",
                thread,
                msg,
                backtrace
            ),
        }
    }));
}

pub fn start_tracing(config: Arc<LoggingConfig>, worker_name: &str, tid: &TransactionId) -> Result<impl Drop> {
    #[allow(dyn_drop)]
    let mut drops: Vec<Box<dyn Drop>> = vec![];

    let file_layer = match config.directory.is_empty() {
        true => None,
        false => {
            let fname = format!("{}.log", config.basename.clone());
            let buff = PathBuf::new();
            ensure_dir(&buff.join(&config.directory))?;
            let dir = match std::fs::canonicalize(config.directory.clone()) {
                Ok(d) => d,
                Err(e) => anyhow::bail!("Failed to canonicalize log file '{}', error: '{}'", config.directory, e),
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
            drops.push(Box::new(guard));
            Some(
                tracing_subscriber::fmt::Layer::default()
                    .with_span_events(str_to_span(&config.spanning)?)
                    .with_timer(ClockWrapper(get_global_clock(tid)?))
                    .with_writer(file_writer)
                    .compact()
                    .json(),
            )
        },
    };

    let stdout_layer = match config.stdout.unwrap_or(false) {
        true => {
            let (stdout, guard) = tracing_appender::non_blocking(std::io::stdout());
            drops.push(Box::new(guard));
            Some(
                tracing_subscriber::fmt::Layer::default()
                    .with_timer(ClockWrapper(get_global_clock(tid)?))
                    .with_writer(stdout)
                    .compact(),
            )
        },
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
        },
        _ => None,
    };
    let subscriber = Registry::default()
        .with(EnvFilter::builder().parse(&config.level)?)
        .with(file_layer)
        .with(stdout_layer)
        .with(energy_layer)
        .with(flame_layer);
    match tracing::subscriber::set_global_default(subscriber) {
        Ok(_) => {
            panic_hook();
            info!(tid=%tid, "Logger initialized");
            Ok(drops)
        },
        Err(e) => {
            warn!(tid=%tid, error=%e, "Global tracing subscriber was already set");
            Ok(vec![])
        },
    }
}
