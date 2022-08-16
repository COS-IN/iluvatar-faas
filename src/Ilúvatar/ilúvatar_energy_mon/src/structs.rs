use serde::{Deserialize, Deserializer};
use time::OffsetDateTime;
use time::serde::rfc3339;

pub struct Span {
  pub timestamp: OffsetDateTime,
  pub level: String,
  pub fields: Fields,
  pub target: String,
  pub span: SubSpan,
  pub spans: Vec<SubSpan>,
  pub name: String,
  pub uuid: String,
}

impl<'de> Deserialize<'de> for Span {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
      D: Deserializer<'de>,
  {
    let json: serde_json::value::Value = serde_json::value::Value::deserialize(deserializer)?;
    let t = json.get("timestamp").expect("timestamp");
    let timestamp = rfc3339::deserialize(t).unwrap();
    let level = json.get("level").expect("level").to_string();
    let target = json.get("target").expect("target").to_string();
    let target = target.strip_prefix("\"").unwrap().strip_suffix("\"").unwrap().to_string();
    let fields = serde_json::from_value(json.get("fields").expect("fields").clone()).unwrap();
    let span: SubSpan = serde_json::from_value(json.get("span").expect("span").clone()).unwrap();
    let spans = serde_json::from_value(json.get("spans").expect("spans").clone()).unwrap();
    let name = format!("{}::{}", target, span.name);
    let uuid = format!("{}::{}", span.tid, name);

    Ok(Span { timestamp, level, target, fields, span, spans, name, uuid })    
  }
}

#[derive(Deserialize)]
pub struct Fields {
  pub message: String,
}

#[derive(Deserialize)]
pub struct SubSpan {
  pub tid: String,
  pub name: String,
}
