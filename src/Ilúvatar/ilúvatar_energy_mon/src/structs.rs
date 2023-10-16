use iluvatar_library::transaction::TransactionId;
use iluvatar_library::utils::calculate_fqdn;
use serde::{Deserialize, Deserializer};

#[derive(Debug)]
pub struct Span {
    pub timestamp: u128,
    pub level: String,
    pub fields: Field,
    pub target: String,
    pub span: SubSpan,
    pub spans: Vec<SubSpan>,
    pub name: String,
    pub uuid: String,
}
#[derive(Deserialize, Debug)]
pub struct Field {
    pub message: String,
}

impl<'de> Deserialize<'de> for Span {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json: serde_json::value::Value = serde_json::value::Value::deserialize(deserializer)?;
        let timestamp = json
            .get("timestamp")
            .expect("timestamp")
            .to_string()
            .parse::<u128>()
            .unwrap();
        let level = json.get("level").expect("level").to_string();
        let target = json.get("target").expect("target").to_string();
        let target = target
            .strip_prefix('\"')
            .unwrap()
            .strip_suffix('\"')
            .unwrap()
            .to_string();
        let fields = serde_json::from_value(json.get("fields").expect("fields").clone()).unwrap();

        let span: SubSpan = serde_json::from_value(json.get("span").expect("span").clone()).unwrap();
        let spans = serde_json::from_value(json.get("spans").expect("spans").clone()).unwrap();
        let name = format!("{}::{}", target, span.name);
        let uuid = format!("{}::{}", span.tid, name);

        Ok(Span {
            timestamp,
            level,
            target,
            fields,
            span,
            spans,
            name,
            uuid,
        })
    }
}

#[derive(Deserialize, Debug)]
pub struct SubSpan {
    pub tid: TransactionId,
    pub name: String,
    pub function_name: Option<String>,
    pub function_version: Option<String>,
    pub fqdn: Option<String>,
}
impl SubSpan {
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
