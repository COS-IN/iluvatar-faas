use super::energy_service::EnergyMonitorService;
use crate::clock::now;
use crate::utils::calculate_fqdn;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::{field::Visit, span::Attributes, Id};
use tracing_subscriber::{layer::Context, Layer};

/// This struct to interact with the tracing subscriber.
/// Reading span information as they are created and closed to compute energy usages.
/// Uses [super::energy_service::EnergyMonitorService] to for energy computation and tracking
pub struct EnergyLayer {
    monitor: Arc<EnergyMonitorService>,
}
impl EnergyLayer {
    pub fn new(worker_name: &str) -> Self {
        EnergyLayer {
            monitor: EnergyMonitorService::boxed(worker_name),
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
        match ctx.span(id) {
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
/// A helper to extract Ilúvatar-specific information from spans, if it is present
pub struct DataExtractorVisitor {
    pub timestamp: Instant,
    pub transaction_id: Option<String>,
    pub function_name: Option<String>,
    pub function_version: Option<String>,
    pub fqdn: Option<String>,
}

impl Default for DataExtractorVisitor {
    fn default() -> Self {
        Self::new()
    }
}

impl DataExtractorVisitor {
    pub fn new() -> Self {
        DataExtractorVisitor {
            timestamp: now(),
            transaction_id: None,
            function_name: None,
            function_version: None,
            fqdn: None,
        }
    }
    pub fn fqdn(&self) -> Option<String> {
        match &self.fqdn {
            Some(f) => Some(f.clone()),
            None => self
                .function_name
                .as_ref()
                .map(|f_n| calculate_fqdn(f_n, self.function_version.as_ref().unwrap())),
        }
    }
}

impl Visit for DataExtractorVisitor {
    // [String] does not match [str] type, so fields we want will fall through to here
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "tid" => self.transaction_id = Some(format!("{value:?}")),
            "function_name" => self.function_name = Some(format!("{value:?}")),
            "function_version" => self.function_version = Some(format!("{value:?}")),
            "fqdn" => self.fqdn = Some(format!("{value:?}")),
            _ => (),
        };
    }
}
