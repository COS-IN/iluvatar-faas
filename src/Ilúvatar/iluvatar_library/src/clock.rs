use std::ops::Add;
use std::sync::Arc;
use time::format_description::FormatItem;
use time::{format_description, OffsetDateTime, UtcOffset};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use crate::tokio_utils::compute_sim_tick_dur;
use crate::transaction::TransactionId;
use anyhow::Result;

pub fn timezone(tid: &TransactionId) -> Result<String> {
    let mut tz_str = match std::fs::read_to_string("/etc/timezone") {
        Ok(t) => t,
        Err(e) => bail_error!(tid=%tid, error=%e, "/etc/timezone is missing!!"),
    };
    tz_str.truncate(tz_str.trim_end().len());
    if tzdb::tz_by_name(&tz_str).is_some() {
        return Ok(tz_str);
    }
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

pub type Clock = Arc<dyn GlobalClock>;
pub trait GlobalClock {
    /// The number of nanoseconds since the unix epoch start, as a String.
    fn now_str(&self) -> Result<String>;
    /// The number of nanoseconds since the unix epoch start.
    fn now(&self) -> OffsetDateTime;
    /// Format the given time
    fn format_time(&self, time: OffsetDateTime) -> Result<String>;
}
impl FormatTime for dyn GlobalClock {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}
fn format_offset_time(clock: &dyn GlobalClock, w: &mut Writer<'_>) -> std::fmt::Result {
    let s = match clock.now_str() {
        Ok(s) => s,
        Err(e) => {
            println!("time formatting error: {}", e);
            return Err(std::fmt::Error {});
        }
    };
    w.write_str(s.as_str())
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
    pub fn boxed(tid: &TransactionId) -> Result<Arc<Self>> {
        Ok(Arc::new(Self::new(tid)?))
    }
}
impl GlobalClock for LocalTime {
    /// The number of nanoseconds since the unix epoch start
    /// As a String
    fn now_str(&self) -> anyhow::Result<String> {
        GlobalClock::format_time(self, self.now())
    }
    /// The number of nanoseconds since the unix epoch start
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc().to_offset(self.local_offset)
    }

    fn format_time(&self, time: OffsetDateTime) -> anyhow::Result<String> {
        Ok(time.format(&self.format)?)
    }
}
impl FormatTime for LocalTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}

/// A struct to serve timestamps as Local time
/// To be used everywhere timestamps are logged externally
/// This matches the times logged in `perf`
pub struct SimulatedTime {
    format: Vec<FormatItem<'static>>,
    start_time: OffsetDateTime,
    elapsed_ticks: u64
}
impl SimulatedTime {
    pub fn new(tid: &TransactionId) -> anyhow::Result<Self> {
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
        Ok(Self {
            format,
            elapsed_ticks: 0,
            start_time: OffsetDateTime::now_utc().to_offset(offset)
        })
    }
    pub fn boxed(tid: &TransactionId) -> anyhow::Result<Box<Self>> {
        let r = Self::new(tid)?;
        Ok(Box::new(r))
    }
    pub fn tick(&self) {
        // TODO: increment tick
        // self.elapsed_ticks += 1;
    }
}
impl Clone for SimulatedTime {
    fn clone(&self) -> Self {
        Self {
            format: self.format.clone(),
            start_time: self.start_time,
            elapsed_ticks: self.elapsed_ticks,
        }
    }
}
impl GlobalClock for SimulatedTime {
    /// The number of nanoseconds since the unix epoch start
    /// As a String
    fn now_str(&self) -> anyhow::Result<String> {
        GlobalClock::format_time(self, self.now())
    }
    /// The number of nanoseconds since the unix epoch start
    fn now(&self) -> OffsetDateTime {
        // TODO: variable tick duration
        self.start_time.add(compute_sim_tick_dur(self.elapsed_ticks))
    }

    fn format_time(&self, time: OffsetDateTime) -> anyhow::Result<String> {
        Ok(time.format(&self.format)?)
    }
}
impl FormatTime for SimulatedTime {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        format_offset_time(self, w)
    }
}