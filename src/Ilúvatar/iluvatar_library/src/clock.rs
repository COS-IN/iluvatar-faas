use crate::transaction::{gen_tid, TransactionId};
use crate::utils::is_simulation;
use anyhow::Result;
use parking_lot::Mutex;
use std::ops::Add;
use std::sync::Arc;
use time::format_description::FormatItem;
use time::{format_description, OffsetDateTime, PrimitiveDateTime, UtcOffset};
use tokio::time::Instant;
use tracing::warn;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

pub type Clock = Arc<dyn GlobalClock + Send + Sync>;
/// The global [Clock]
static CLOCK: Mutex<Option<Clock>> = Mutex::new(None);

/// Gets the current global clock. Creates a new [Clock] if not present.
pub fn get_global_clock(tid: &TransactionId) -> Result<Clock> {
    if let Some(rt) = CLOCK.lock().as_ref() {
        return Ok(rt.clone());
    }
    let clk: Clock = match is_simulation() {
        true => SimulatedTime::boxed(tid)?,
        false => LocalTime::boxed(tid)?,
    };
    *CLOCK.lock() = Some(clk.clone());
    Ok(clk)
}

/// Get the current [Instant]
#[inline(always)]
pub fn now() -> Instant {
    // allow here because we want all code to use this method
    #[allow(clippy::disallowed_methods)]
    Instant::now()
}
pub fn timezone(tid: &TransactionId) -> Result<String> {
    let mut tz_str = match std::fs::read_to_string("/etc/timezone") {
        Ok(t) => t,
        Err(e) => {
            warn!(tid=tid, error=%e, "/etc/timezone is missing, using default");
            "America/Indiana/Indianapolis".to_owned()
        },
    };
    tz_str.truncate(tz_str.trim_end().len());
    if tzdb::tz_by_name(&tz_str).is_some() {
        return Ok(tz_str);
    }
    let sections: Vec<&str> = tz_str.split('/').collect();
    if sections.len() == 2 {
        anyhow::bail!("Unknown timezone string {}", tz_str)
    }
    let tz_str_2 = format!("{}/{}", sections[0], sections[2]);
    match tzdb::tz_by_name(&tz_str_2) {
        Some(_) => Ok(tz_str_2),
        None => anyhow::bail!("local timezone string was invalid: {}", tz_str),
    }
}

pub trait GlobalClock: Send + Sync {
    /// The number of nanoseconds since the unix epoch start, as a String.
    fn now_str(&self) -> Result<String>;
    /// The number of nanoseconds since the unix epoch start.
    fn now(&self) -> OffsetDateTime;
    /// Format the given time
    fn format_time(&self, time: OffsetDateTime) -> Result<String>;
}
impl FormatTime for dyn GlobalClock {
    #[inline(always)]
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}
/// Dummy wrapper for [Clock] to make logging framework happy.
pub struct ClockWrapper(pub Arc<dyn GlobalClock>);
impl FormatTime for ClockWrapper {
    #[inline(always)]
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}
impl GlobalClock for ClockWrapper {
    #[inline(always)]
    fn now_str(&self) -> Result<String> {
        self.0.now_str()
    }
    #[inline(always)]
    fn now(&self) -> OffsetDateTime {
        self.0.now()
    }
    #[inline(always)]
    fn format_time(&self, time: OffsetDateTime) -> Result<String> {
        self.0.format_time(time)
    }
}

fn format_offset_time(clock: &dyn GlobalClock, w: &mut Writer<'_>) -> std::fmt::Result {
    let s = match clock.now_str() {
        Ok(s) => s,
        Err(e) => {
            println!("time formatting error: {}", e);
            return Err(std::fmt::Error {});
        },
    };
    w.write_str(s.as_str())
}

/// A struct to serve timestamps as Local time.
/// To be used everywhere timestamps are logged externally.
/// This matches the times logged in `perf`.
struct LocalTime {
    format: Vec<FormatItem<'static>>,
    local_offset: UtcOffset,
}
impl LocalTime {
    pub fn new(tid: &TransactionId) -> Result<Self> {
        let format = format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]")?;
        #[allow(clippy::disallowed_methods)]
        let now = OffsetDateTime::now_utc();
        let offset = load_local_offset(now, tid)?;
        Ok(LocalTime {
            format,
            local_offset: offset,
        })
    }
    pub fn boxed(tid: &TransactionId) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::new(tid)?))
    }
}
impl GlobalClock for LocalTime {
    fn now_str(&self) -> Result<String> {
        GlobalClock::format_time(self, self.now())
    }
    fn now(&self) -> OffsetDateTime {
        #[allow(clippy::disallowed_methods)]
        OffsetDateTime::now_utc().to_offset(self.local_offset)
    }
    fn format_time(&self, time: OffsetDateTime) -> Result<String> {
        Ok(time.format(&self.format)?)
    }
}
impl FormatTime for LocalTime {
    #[inline(always)]
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}

/// A struct to serve timestamps using the same format as [LocalTime], but running inside the simulation.
struct SimulatedTime {
    format: Vec<FormatItem<'static>>,
    start_time: OffsetDateTime,
    tokio_elapsed: Instant,
}
impl SimulatedTime {
    pub fn new(tid: &TransactionId) -> Result<Self> {
        let format = format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]")?;
        let local_clock = LocalTime::new(tid)?;
        Ok(Self {
            format,
            // simulated clock starts using current system time
            start_time: local_clock.now(),
            tokio_elapsed: now(),
        })
    }
    pub fn boxed(tid: &TransactionId) -> Result<Arc<Self>> {
        let r = Self::new(tid)?;
        Ok(Arc::new(r))
    }
}
impl GlobalClock for SimulatedTime {
    fn now_str(&self) -> Result<String> {
        GlobalClock::format_time(self, self.now())
    }
    fn now(&self) -> OffsetDateTime {
        self.start_time.add(self.tokio_elapsed.elapsed())
    }
    fn format_time(&self, time: OffsetDateTime) -> Result<String> {
        Ok(time.format(&self.format)?)
    }
}
impl FormatTime for SimulatedTime {
    #[inline(always)]
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}

/// Parses the times returned from a Python container into Rust objects
pub struct ContainerTimeFormatter {
    py_tz_formatter: Vec<FormatItem<'static>>,
    py_formatter: Vec<FormatItem<'static>>,
    local_offset: UtcOffset,
    clock: Clock,
}
impl ContainerTimeFormatter {
    pub fn new(tid: &TransactionId) -> Result<Self> {
        let py_tz_formatter =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]:[subsecond]+[offset_hour]")?;
        let py_formatter = format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]:[subsecond]+")?;
        let clock = get_global_clock(&gen_tid())?;
        Ok(ContainerTimeFormatter {
            py_tz_formatter,
            py_formatter,
            local_offset: load_local_offset(clock.now(), tid)?,
            clock,
        })
    }

    /// Python format: "%Y-%m-%d %H:%M:%S:%f+%z"
    pub fn parse_python_container_time(&self, date: &str) -> Result<OffsetDateTime> {
        if date
            .chars()
            .last()
            .ok_or_else(|| anyhow::anyhow!("Passed date was empty"))?
            == '+'
        {
            Ok(PrimitiveDateTime::parse(date, &self.py_formatter)?.assume_utc())
        } else {
            Ok(OffsetDateTime::parse(date, &self.py_tz_formatter)?)
        }
    }
}
impl GlobalClock for ContainerTimeFormatter {
    #[inline(always)]
    fn now_str(&self) -> Result<String> {
        let time = self.now();
        self.format_time(time)
    }
    #[inline(always)]
    fn now(&self) -> OffsetDateTime {
        self.clock.now().to_offset(self.local_offset)
    }
    #[inline(always)]
    fn format_time(&self, time: OffsetDateTime) -> Result<String> {
        Ok(time.format(&self.py_formatter)?)
    }
}

fn load_local_offset(from_time: OffsetDateTime, tid: &TransactionId) -> Result<UtcOffset> {
    let tz_str = timezone(tid)?;
    let time_zone = match tzdb::tz_by_name(&tz_str) {
        Some(t) => t,
        None => anyhow::bail!("parsed local timezone string was invalid: {}", tz_str),
    };
    let tm = match time_zone.find_local_time_type(from_time.unix_timestamp()) {
        Ok(t) => t,
        Err(e) => bail_error!(tid=tid, error=%e, "Failed to find time zone type"),
    };
    Ok(UtcOffset::from_whole_seconds(tm.ut_offset())?)
}
