use std::{time::SystemTime, sync::Arc};
use tracing::{Id, span::Attributes, field::Visit};
use tracing_subscriber::{Layer, layer::Context};
use crate::{utils::calculate_fqdn, graphite::GraphiteConfig};
use super::energy_monitor::EnergyMonitor;

pub struct EnergyLayer {
  monitor: Arc<EnergyMonitor>
}
impl EnergyLayer {
  pub fn new(graphite_cfg: Arc<GraphiteConfig>) -> Self {
    EnergyLayer {
      monitor: EnergyMonitor::boxed(graphite_cfg)
    }
  }
}

// https://burgers.io/custom-logging-in-rust-using-tracing
impl<S> Layer<S> for EnergyLayer
where
  S: tracing::Subscriber,
  S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
  fn on_close(&self, id: Id, ctx: Context<'_, S>) {
    match ctx.span(&id) {
      Some(s) => {
        let name = s.name();
        let target = s.metadata().target();
        self.monitor.span_close(id.into_u64(), name, target);
      },
      None => (),
    };
  }

  fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
    match ctx.span(&id) {
      Some(s) => {
        let name = s.name();
        let target = s.metadata().target();
        let mut visitor = DataExtractorVisitor::new();
        attrs.record(&mut visitor);
        self.monitor.span_create(id.into_u64(), visitor, name, target);
      },
      None => (),
    };
  }
}

#[derive(Debug)]
pub struct DataExtractorVisitor {
  pub timestamp: SystemTime,
  pub transaction_id: Option<String>,
  pub function_name: Option<String>,
  pub function_version: Option<String>,
  pub fqdn: Option<String>,
}

impl DataExtractorVisitor {
  pub fn new() -> Self {
    DataExtractorVisitor {
      timestamp: SystemTime::now(),
      transaction_id: None,
      function_name: None,
      function_version: None,
      fqdn: None,
    }
  }
  pub fn fqdn(&self) -> Option<String> {
    match &self.fqdn {
      Some(f) => Some(f.clone()),
      None => match &self.function_name {
        Some(f_n) => {
          Some(calculate_fqdn(f_n, self.function_version.as_ref().unwrap()))
        },
        None => None,
      },
    }
  }
}

impl Visit for DataExtractorVisitor {
  // [String] does not match [str] type, so they will fall through to here
  fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
    match field.name() {
      "tid" => self.transaction_id = Some(format!("{value:?}")),
      "function_name" => self.function_name = Some(format!("{value:?}")),
      "function_version" => self.function_version = Some(format!("{value:?}")),
      "fqdn" => self.fqdn = Some(format!("{value:?}")),
      _ => ()
    };
  }
}
