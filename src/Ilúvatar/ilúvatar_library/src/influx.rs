use std::{collections::HashMap, sync::Arc};
use influxdb2::{Client, RequestError};
use influxdb2::models::{bucket::PostBucketRequest, retention_rule::{RetentionRule, Type}};
use influxdb2::api::organization::ListOrganizationRequest;
use anyhow::Result;
use tracing::{info,debug};
use crate::transaction::TransactionId;

#[derive(Debug, serde::Deserialize)]
pub struct InfluxConfig {
  pub host: String,
  pub org: String,
  pub token: String,
}

pub struct InfluxService {
  _config: Arc<InfluxConfig>,
  client: Client,
  internal_id: String,
}

const FUNCTIONS_BUCKET: &str = "functions";
const WORKERS_BUCKET: &str = "workers";
const THREE_HOURS_IN_SEC: i32 = 60 * 60 * 3;

impl InfluxService {
  pub async fn new(config: Arc<InfluxConfig>, tid: &TransactionId) -> Result<Option<Self>> {
    if config.host == "" && config.org == "" && config.token == "" {
      debug!(tid=%tid, "Disabling influx, all configs are nones");
      return Ok(None);
    }
    let client = Client::new(&config.host, &config.org, &config.token);
    let org_id = Self::get_internal_organization_id(&config, &client, tid).await?;
    Ok(Some(Self {
      client,
      _config: config,
      internal_id: org_id,
    }.ensure_buckets(tid).await?))
  }

  async fn get_internal_organization_id(config: &Arc<InfluxConfig>, client: &Client, tid: &TransactionId) -> Result<String> {
    let orgs = client.list_organizations(ListOrganizationRequest::default()).await?;
    for org in orgs.orgs {
      if org.name == config.org {
        match org.id {
          Some(org_id) => return Ok(org_id),
          None => bail_error!(tid=%tid, "Found an organization with a matching name, but it had no ID!")
        }
      }
    }
    anyhow::bail!("Unable to find an organization with a matching name!")
  }

  /// Raise an error if the map is missing either the key or has an unexpected value for the key 
  fn has_k_v(&self, map: &HashMap<String,String>, key: &str, value: &str) -> Result<()> {
    match map.get(key) {
      Some(t) => {
        if t != value {
          anyhow::bail!("Response from Influx on key '{}' had unexpected value '{}'", key, t)
        }
        Ok(())
      },
      None => anyhow::bail!("Response from Influx did not have expected member '{}'", key),
    }
  }

  /// Raise the error if it is an actual problem with Influx
  /// Ignore duplicate bucket creation messages
  fn handle_ensure_bucket_error(&self, name: &str, e: RequestError, tid: &TransactionId) -> Result<()> {
    match e {
      RequestError::ReqwestProcessing { source } => anyhow::bail!(source.to_string()),
      RequestError::Http { status, text } => match status {
          reqwest::StatusCode::UNPROCESSABLE_ENTITY => match serde_json::from_str::<HashMap<String,String>>(&text) {
              Ok(m) => {
                self.has_k_v(&m, "code", "conflict")?;
                self.has_k_v(&m, "message", format!("bucket with name {} already exists", name).as_str())?;
                info!(tid=%tid, "Bucket {} already exists", name);
                Ok(())
              },
              Err(_) => anyhow::bail!(format!("Unknown error format: '{}'", text)),
            }
          _ => anyhow::bail!(format!("HTTP error on creating bucket: {} {}", status, text))
        },
      RequestError::Serializing { source } => anyhow::bail!(source.to_string()),
      RequestError::Deserializing { text } => anyhow::bail!(text),
    }
  }

  async fn ensure_buckets(self, tid: &TransactionId) -> Result<Self> {
    match self.client.create_bucket(Some(PostBucketRequest {
      org_id: self.internal_id.clone(),
      name: FUNCTIONS_BUCKET.to_string(),
      description: None,
      rp: None,
      retention_rules: vec![RetentionRule::new(Type::Expire, THREE_HOURS_IN_SEC)],
    })).await {
      Ok(_) => info!(tid=%tid, "Functions bucket created"),
      Err(e) => self.handle_ensure_bucket_error(FUNCTIONS_BUCKET, e, tid)?,
    };

    match self.client.create_bucket(Some(PostBucketRequest {
      org_id: self.internal_id.clone(),
      name: WORKERS_BUCKET.to_string(),
      description: None,
      rp: None,
      retention_rules: vec![RetentionRule::new(Type::Expire, THREE_HOURS_IN_SEC)],
    })).await {
      Ok(_) => info!(tid=%tid, "Workers bucket created"),
      Err(e) => self.handle_ensure_bucket_error(WORKERS_BUCKET, e, tid)?,
    };
    Ok(self)
  }
}