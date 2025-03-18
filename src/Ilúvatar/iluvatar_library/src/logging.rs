use crate::bail_error;
use crate::clock::{get_global_clock, ClockWrapper};
use crate::transaction::TransactionId;
use crate::utils::file_utils::ensure_dir;
use anyhow::Result;
use std::io::ErrorKind;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use tracing::span::Attributes;
use tracing::{info, warn, Event, Id, Metadata, Subscriber};
use tracing_flame::FlameLayer;
use tracing_subscriber::filter::FilterExt;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, registry::Registry, Layer};

#[derive(Debug, serde::Serialize, serde::Deserialize, Default, Clone)]
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
    /// The recommended value is `NONE`, to reduce overwhelming log size.
    /// Another option is `NEW+CLOSE`, which creates logs on span (function) execution and completion, allowing tracking of when a function call starts and stops.
    /// More expressive span capturing naturally means larger log files.
    pub spanning: String,
    /// A file name to put flame trace data in, file will be placed in [Self::directory] if present, or local directory.
    /// See (here)<https://docs.rs/tracing-flame/latest/tracing_flame/index.html> for details on how this works.
    /// If empty, do not record flame data.
    /// Only used on live system.
    #[serde(default)]
    pub flame: String,
    /// Use live span information to track invocation and background energy usage.
    /// These will then be reported to influx.
    #[serde(default)]
    pub span_energy_monitoring: bool,
    /// Include currently entered spans when logging JSON messages to file.
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

struct WorkerSpanFilter {
    worker: String,
}
struct GetValue {
    worker_name: Option<String>,
}
impl tracing::field::Visit for GetValue {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "worker" {
            self.worker_name = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {
        // Mandatory to implement, do nothing
    }
}
impl<S: Subscriber + for<'span> LookupSpan<'span>> Filter<S> for WorkerSpanFilter {
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        true
    }

    fn event_enabled(&self, _event: &Event<'_>, cx: &Context<'_, S>) -> bool {
        let curr = cx.current_span();
        if let Some(id) = curr.id() {
            if let Some(act_curr_span) = cx.span(id) {
                for span in act_curr_span.scope() {
                    if span.name() == "enter_worker" {
                        if let Some(spec_span) = cx.span(&span.id()) {
                            if let Some(filter) = spec_span.extensions().get::<WorkerSpanFilter>() {
                                // trace event corresponds to _this_ worker
                                return filter.worker == self.worker;
                            }
                            // trace event corresponds to another worker
                            break;
                        }
                    }
                }
            }
        }
        false
    }
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, cx: Context<'_, S>) {
        if let Some(span) = cx.span(id) {
            if span.extensions().get::<WorkerSpanFilter>().is_some() {
                // someone set up this span already, be lazy
                return;
            }
            if attrs.metadata().name() == "enter_worker" {
                let mut finder = GetValue { worker_name: None };
                attrs.values().record(&mut finder);
                if let Some(worker_name) = finder.worker_name {
                    // set WorkerSpanFilter on this span, use `replace` in case it changes beneath us
                    // other thread will be putting in identical value
                    span.extensions_mut().replace(WorkerSpanFilter { worker: worker_name });
                }
            }
        }
    }
}
fn worker_span_filter(worker_name: &str) -> WorkerSpanFilter {
    WorkerSpanFilter {
        worker: worker_name.to_string(),
    }
}

#[allow(dyn_drop)]
fn file_logger<S: Subscriber + for<'span> LookupSpan<'span>, P: AsRef<Path>>(
    folder_path: P,
    base_filename: &str,
    span_events: &str,
    with_span_list: bool,
    filter: Option<Box<dyn Filter<S> + Send + Sync + 'static>>,
    tid: &TransactionId,
) -> Result<(Box<dyn Layer<S> + Send + Sync + 'static>, Box<dyn Drop>)> {
    let fname = format!("{}.log", base_filename);
    let dir = match std::fs::canonicalize(&folder_path) {
        Ok(d) => d,
        Err(e) => match e.kind() {
            ErrorKind::NotFound => {
                info!(tid = tid, "making log dir");
                ensure_dir(&folder_path)?;
                std::fs::canonicalize(&folder_path)?
            },
            _ => anyhow::bail!(
                "Failed to canonicalize log file '{:?}', error: '{}'",
                folder_path.as_ref().to_str(),
                e
            ),
        },
    };

    let full_path = dir.join(&fname);
    println!("Logging to {}", full_path.to_string_lossy());
    if full_path.exists() {
        match std::fs::remove_file(full_path) {
            Ok(_) => (),
            Err(e) => {
                if let Some(ow_e) = e.raw_os_error() {
                    // error 2 means file was gone, deleted from under us. Ignore
                    if ow_e != 2 {
                        anyhow::bail!("Failed to remove old log file because '{}'", e);
                    } else {
                        warn!(tid=tid, error=%e, "Log file was deleted from under while this was trying to delete");
                    }
                }
            },
        };
    }

    let appender = tracing_appender::rolling::never(dir, fname);
    let (file_writer, guard) = tracing_appender::non_blocking(appender);

    let layer = tracing_subscriber::fmt::Layer::default()
        .with_span_events(str_to_span(span_events)?)
        .with_timer(ClockWrapper(get_global_clock(tid)?))
        .with_writer(file_writer)
        .json()
        .with_span_list(with_span_list)
        .with_filter(filter);
    Ok((layer.boxed(), Box::new(guard)))
}

/// Simulation gets a special logging setup because we want to separate logs from each component.
/// In live version this comes naturally because each runs in their own process and has private setup.
pub fn start_simulation_tracing(
    config: &Arc<LoggingConfig>,
    cluster: bool,
    num_workers: usize,
    worker_name: &str,
    tid: &TransactionId,
) -> Result<impl Drop> {
    #[allow(dyn_drop)]
    let mut drops: Vec<Box<dyn Drop>> = vec![];
    let mut layers = vec![];

    for worker_id in 0..num_workers {
        // filter logs from each worker to separate files
        if !config.directory.is_empty() {
            let file_name = format!("{}_{}", worker_name, worker_id);
            // Look at the span to know what worker we're in
            let (file_layer, guard) = file_logger(
                &config.directory,
                &file_name,
                &config.spanning,
                config.include_spans_json,
                Some(FilterExt::boxed(worker_span_filter(&file_name))),
                tid,
            )?;
            drops.push(guard);
            layers.push(file_layer);
        }
    }
    if cluster {
        // filter controller logs
        if !config.directory.is_empty() {
            let span_filter = format!(
                "iluvatar_controller_library={},iluvatar_controller={}",
                config.level, config.level
            );
            let (file_layer, guard) = file_logger(
                &config.directory,
                "controller",
                &config.spanning,
                config.include_spans_json,
                Some(FilterExt::boxed(EnvFilter::builder().parse(span_filter)?)),
                tid,
            )?;
            drops.push(guard);
            layers.push(file_layer);
        }
    }
    // filter load gen logs
    if !config.directory.is_empty() {
        let span_filter = format!("iluvatar_load_gen={}", config.level);
        let (file_layer, guard) = file_logger(
            &config.directory,
            "load_gen",
            &config.spanning,
            config.include_spans_json,
            Some(FilterExt::boxed(EnvFilter::builder().parse(span_filter)?)),
            tid,
        )?;
        drops.push(guard);
        layers.push(file_layer);
    }
    if config.stdout.unwrap_or(false) {
        stdout_layer(&mut layers, &mut drops, tid)?;
    }
    let subscriber = Registry::default()
        .with(EnvFilter::builder().parse(&config.level)?)
        .with(layers);
    match tracing::subscriber::set_global_default(subscriber) {
        Ok(_) => {
            panic_hook();
            info!(tid = tid, "Logger initialized");
            #[cfg(not(feature = "full_spans"))]
            {
                println!("WARN: 'full_spans' feature is not enabled, worker logs may be lost or unreliable");
                warn!(
                    tid = tid,
                    "'full_spans' feature is not enabled, worker logs may be lost or unreliable"
                );
            }
            Ok(drops)
        },
        Err(e) => {
            #[cfg(not(feature = "full_spans"))]
            {
                println!("WARN: 'full_spans' feature is not enabled, worker logs may be lost or unreliable");
                warn!(
                    tid = tid,
                    "'full_spans' feature is not enabled, worker logs may be lost or unreliable"
                );
            }
            warn!(tid=tid, error=%e, "Global tracing subscriber was already set");
            Ok(vec![])
        },
    }
}

#[allow(dyn_drop)]
fn stdout_layer<S: Subscriber + for<'span> LookupSpan<'span>>(
    layers: &mut Vec<Box<(dyn Layer<S> + Send + Sync + 'static)>>,
    drops: &mut Vec<Box<dyn Drop>>,
    tid: &TransactionId,
) -> Result<()> {
    let (stdout, guard) = tracing_appender::non_blocking(std::io::stdout());
    drops.push(Box::new(guard));
    layers.push(
        tracing_subscriber::fmt::Layer::default()
            .with_timer(ClockWrapper(get_global_clock(tid)?))
            .with_writer(stdout)
            .compact()
            .boxed(),
    );
    Ok(())
}

pub fn start_tracing(config: &Arc<LoggingConfig>, tid: &TransactionId) -> Result<impl Drop> {
    #[allow(dyn_drop)]
    let mut drops: Vec<Box<dyn Drop>> = vec![];
    let mut layers = vec![];
    if !config.directory.is_empty() {
        let (file_layer, guard) = file_logger(
            &config.directory,
            &config.basename,
            &config.spanning,
            config.include_spans_json,
            None,
            tid,
        )?;
        drops.push(guard);
        layers.push(file_layer);
    };

    if config.stdout.unwrap_or(false) {
        stdout_layer(&mut layers, &mut drops, tid)?;
    }

    if !config.flame.is_empty() {
        let flame_path = PathBuf::from(&config.directory).join(&config.flame);
        let (flame_layer, flame_guard) = match FlameLayer::with_file(flame_path) {
            Ok(l) => l,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to make FlameLayer"),
        };
        drops.push(Box::new(flame_guard));
        layers.push(
            flame_layer
                .with_threads_collapsed(true)
                .with_file_and_line(true)
                .boxed(),
        );
    }
    let subscriber = Registry::default()
        .with(EnvFilter::builder().parse(&config.level)?)
        .with(layers);
    match tracing::subscriber::set_global_default(subscriber) {
        Ok(_) => {
            panic_hook();
            info!(tid = tid, "Logger initialized");
            Ok(drops)
        },
        Err(e) => {
            warn!(tid=tid, error=%e, "Global tracing subscriber was already set");
            Ok(vec![])
        },
    }
}
