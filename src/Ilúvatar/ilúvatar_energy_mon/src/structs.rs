use serde::Deserialize;
use time::OffsetDateTime;

#[derive(Deserialize)]
pub struct Span {
  #[serde(with = "time::serde::rfc3339")]
  pub timestamp: OffsetDateTime,
  pub level: String,
  pub fields: Fields,
  pub target: String,
  pub span: SubSpan,
  pub spans: Vec<SubSpan>,
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
