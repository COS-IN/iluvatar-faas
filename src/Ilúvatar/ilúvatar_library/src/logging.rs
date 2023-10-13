use crate::bail_error;
use crate::energy::energy_layer::EnergyLayer;
use crate::transaction::TransactionId;
use crate::utils::file_utils::ensure_dir;
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use time::format_description::FormatItem;
use time::{format_description, OffsetDateTime, UtcOffset};
use tracing_flame::FlameLayer;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{prelude::*, Registry};

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

pub fn start_tracing(config: Arc<LoggingConfig>, worker_name: &String, tid: &TransactionId) -> Result<impl Drop> {
    #[allow(dyn_drop)]
    let mut drops: Vec<Box<dyn Drop>> = Vec::new();
    let (non_blocking, _guard) = match config.directory.as_ref() {
        None => tracing_appender::non_blocking(std::io::stdout()),
        Some(log_dir) => {
            let fname = format!("{}.log", config.basename.clone());
            let buff = PathBuf::new();
            ensure_dir(&buff.join(&log_dir))?;
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
            if flame.len() == 0 {
                layers.with(None)
            } else {
                let flame_path = match config.directory.as_ref() {
                    Some(p) => PathBuf::from(p).join(flame),
                    None => PathBuf::from(flame),
                };
                let (mut flame_layer, _flame_guard) = match FlameLayer::with_file(&flame_path) {
                    Ok(l) => l,
                    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make FlameLayer"),
                };
                flame_layer = flame_layer.with_threads_collapsed(true).with_file_and_line(true);
                drops.push(Box::new(_flame_guard));
                layers.with(Some(flame_layer))
            }
        }
    };

    let writer_layer = tracing_subscriber::fmt::layer()
        .with_span_events(str_to_span(&config.spanning))
        .with_timer(LocalTime::new(tid)?)
        .with_writer(non_blocking);
    match config.directory {
        None => layers.with(writer_layer).init(),
        Some(_) => layers.with(writer_layer.json()).init(),
    };
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
    let sections: Vec<&str> = tz_str.split("/").collect();
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
                println!("time formatting error: {}", e);
                return Err(std::fmt::Error {});
            }
        };
        w.write_str(s.as_str())
    }
}
