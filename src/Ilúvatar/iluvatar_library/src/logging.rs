use crate::bail_error;
use crate::energy::energy_layer::EnergyLayer;
use crate::transaction::TransactionId;
use crate::utils::file_utils::ensure_dir;
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use time::format_description::FormatItem;
use time::{format_description, OffsetDateTime, UtcOffset};
use tracing::error;
use tracing_flame::FlameLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

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

pub fn start_tracing(config: Arc<LoggingConfig>, worker_name: &str, tid: &TransactionId) -> Result<impl Drop> {
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

    let subscriber = tracing_subscriber::fmt()
        .with_timer(LocalTime::new(tid)?)
        .with_span_events(str_to_span(&config.spanning)?)
        .with_env_filter(EnvFilter::builder().parse(&config.level)?)
        .with_writer(file_writer)
        .json()
        .finish();

    let subscriber = subscriber.with(stdout_layer).with(energy_layer).with(flame_layer);
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(drops)
}

pub fn timezone(tid: &TransactionId) -> Result<String> {
    let mut tz_str = match std::fs::read_to_string("/etc/timezone") {
        Ok(t) => t,
        Err(e) => bail_error!(tid=%tid, error=%e, "/etc/timezone doesn ot exist!!"),
    };
    tz_str.truncate(tz_str.trim_end().len());
    match tzdb::tz_by_name(&tz_str) {
        Some(_) => return Ok(tz_str),
        None => (),
    };
    let sections: Vec<&str> = tz_str.split('/').collect();
    if sections.len() == 2 {
        anyhow::bail!("Unknown timezome string {}", tz_str)
    }
    let tz_str_2 = format!("{}/{}", sections[0], sections[2]);
    match tzdb::tz_by_name(&tz_str_2) {
        Some(_) => Ok(tz_str_2),
        None => anyhow::bail!("local timezone string was invalid: {}", tz_str),
    }
}

/// A struct to serve timestamps as Local time
/// To be used everywhere timestamps are logged externally
/// This matches the times logged in `perf`
pub struct LocalTime {
    format: Vec<FormatItem<'static>>,
    local_offset: UtcOffset,
}
impl LocalTime {
    pub fn new(tid: &TransactionId) -> Result<Self> {
        let format = format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]")?;
        let now = OffsetDateTime::now_utc();
        let tz_str = timezone(tid)?;
        let time_zone = match tzdb::tz_by_name(&tz_str) {
            Some(t) => t,
            None => anyhow::bail!("parsed local timezone string was invalid: {}", tz_str),
        };
        let tm = match time_zone.find_local_time_type(now.unix_timestamp()) {
            Ok(t) => t,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to find time zone type"),
        };
        let offset = UtcOffset::from_whole_seconds(tm.ut_offset())?;
        Ok(LocalTime {
            format,
            local_offset: offset,
        })
    }
    /// The number of nanoseconds since the unix epoch start
    pub fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc().to_offset(self.local_offset)
    }
    /// The number of nanoseconds since the unix epoch start
    /// As a String
    pub fn now_str(&self) -> Result<String> {
        let time = self.now();
        self.format_time(time)
    }

    pub fn format_time(&self, time: OffsetDateTime) -> Result<String> {
        Ok(time.format(&self.format)?)
    }
}
impl tracing_subscriber::fmt::time::FormatTime for LocalTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let s = match self.now_str() {
            Ok(s) => s,
            Err(e) => {
                error!("time formatting error: {}", e);
                return Err(std::fmt::Error {});
            }
        };
        w.write_str(s.as_str())
    }
}
