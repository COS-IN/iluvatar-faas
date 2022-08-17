use tracing::{Id, span::Attributes, field::Visit};
use tracing_subscriber::{Layer, layer::Context};

pub struct EnergyLayer {}

// https://burgers.io/custom-logging-in-rust-using-tracing
impl<S> Layer<S> for EnergyLayer
where
  S: tracing::Subscriber,
  S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
  fn on_close(&self, id: Id, ctx: Context<'_, S>) {
    match ctx.span(&id) {
      Some(s) => {
        let fields = s.fields();
        let name = s.name();
        println!("on_close name: {}; id: {:?}; tid: {:?}", name, id, fields.field("tid"))
      },
      None => println!("ERROR!! on_close didn't have a span; id: {:?}", id),
    };
  }

  fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
    let mut visitor = DataExtractorVisitor::new();
    attrs.record(&mut visitor);
    match ctx.span(&id) {
      Some(s) => {
        let fields = s.fields();
        let name = s.name();
        println!("on_new_span name: {}; id: {:?}; fields: {:?}; visitor: {:?}", name, id, fields, visitor)
      },
      None => println!("ERROR!! on_new_span didn't have a span; id: {:?}; visitor: {:?}", id, visitor),
    };
  }
}

#[derive(Debug)]
pub struct DataExtractorVisitor {
  transaction_id: Option<String>,
  function_name: Option<String>,
  function_version: Option<String>,
  fqdn: Option<String>,
}

impl DataExtractorVisitor {
  pub fn new() -> Self {
    DataExtractorVisitor {
      transaction_id: None,
      function_name: None,
      function_version: None,
      fqdn: None,
    }
  }
}

impl Visit for DataExtractorVisitor {
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
