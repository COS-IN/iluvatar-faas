use crate::transaction::TransactionId;
use anyhow::Result;
use influxdb2::api::organization::ListOrganizationRequest;
use influxdb2::models::{
    bucket::PostBucketRequest,
    retention_rule::{RetentionRule, Type},
};
use influxdb2::{Client, FromMap, RequestError};
use std::{collections::HashMap, sync::Arc};
use tracing::info;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct InfluxConfig {
    pub host: String,
    pub org: String,
    pub token: String,
    /// If using influx is enabled
    pub enabled: bool,
    /// Frequency to send/receive updates to/from InfluxDB
    pub update_freq_ms: u64,
}

pub struct InfluxClient {
    _config: Arc<InfluxConfig>,
    client: Client,
    internal_org_id: String,
}

pub const FUNCTIONS_BUCKET: &str = "functions";
pub const WORKERS_BUCKET: &str = "workers";
const THREE_HOURS_IN_SEC: i32 = 60 * 60 * 3;

impl InfluxClient {
    pub async fn new(config: Arc<InfluxConfig>, tid: &TransactionId) -> Result<Option<Arc<Self>>> {
        if !config.enabled {
            info!(tid = tid, "Influx disabled, skipping client creation");
            return Ok(None);
        }
        if config.host.is_empty() || config.org.is_empty() || config.token.is_empty() {
            anyhow::bail!("Influx host, org, or token are empty");
        }
        let client = Client::new(&config.host, &config.org, &config.token);
        let internal_org_id = Self::get_internal_organization_id(&config, &client, tid).await?;
        Ok(Some(Arc::new(
            Self {
                client,
                internal_org_id,
                _config: config,
            }
            .ensure_buckets(tid)
            .await?,
        )))
    }

    async fn get_internal_organization_id(
        config: &Arc<InfluxConfig>,
        client: &Client,
        tid: &TransactionId,
    ) -> Result<String> {
        let orgs = client.list_organizations(ListOrganizationRequest::default()).await?;
        for org in orgs.orgs {
            if org.name == config.org {
                match org.id {
                    Some(org_id) => return Ok(org_id),
                    None => {
                        bail_error!(
                            tid = tid,
                            "Found an organization with a matching name, but it had no ID!"
                        )
                    },
                }
            }
        }
        anyhow::bail!("Unable to find an organization with a matching name!")
    }

    /// Raise an error if the map is missing either the key or has an unexpected value for the key
    fn has_k_v(&self, map: &HashMap<String, String>, key: &str, value: &str) -> Result<()> {
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
            RequestError::Http { status, text } => match status.as_u16() {
                422 => {
                    // UNPROCESSABLE_ENTITY
                    match serde_json::from_str::<HashMap<String, String>>(&text) {
                        Ok(m) => {
                            self.has_k_v(&m, "code", "conflict")?;
                            self.has_k_v(
                                &m,
                                "message",
                                format!("bucket with name {} already exists", name).as_str(),
                            )?;
                            info!(tid = tid, "Bucket {} already exists", name);
                            Ok(())
                        },
                        Err(_) => anyhow::bail!(format!("Unknown error format: '{}'", text)),
                    }
                },
                _ => anyhow::bail!(format!("HTTP error on creating bucket: {} {}", status, text)),
            },
            RequestError::Serializing { source } => anyhow::bail!(source.to_string()),
            RequestError::Deserializing { text } => anyhow::bail!(text),
        }
    }

    async fn ensure_buckets(self, tid: &TransactionId) -> Result<Self> {
        match self
            .client
            .create_bucket(Some(PostBucketRequest {
                org_id: self.internal_org_id.clone(),
                name: FUNCTIONS_BUCKET.to_string(),
                description: None,
                rp: None,
                retention_rules: vec![RetentionRule::new(Type::Expire, THREE_HOURS_IN_SEC)],
            }))
            .await
        {
            Ok(_) => info!(tid = tid, "Functions bucket created"),
            Err(e) => self.handle_ensure_bucket_error(FUNCTIONS_BUCKET, e, tid)?,
        };

        match self
            .client
            .create_bucket(Some(PostBucketRequest {
                org_id: self.internal_org_id.clone(),
                name: WORKERS_BUCKET.to_string(),
                description: None,
                rp: None,
                retention_rules: vec![RetentionRule::new(Type::Expire, THREE_HOURS_IN_SEC)],
            }))
            .await
        {
            Ok(_) => info!(tid = tid, "Workers bucket created"),
            Err(e) => self.handle_ensure_bucket_error(WORKERS_BUCKET, e, tid)?,
        };
        Ok(self)
    }

    /// Write the given data to the bucket.
    /// data must be in valid (InfluxDB line protocol)<https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_reference/>
    pub async fn write_data(&self, bucket: &str, line_data: String) -> Result<()> {
        match self
            .client
            .write_line_protocol(&self.internal_org_id, bucket, line_data)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => anyhow::bail!("{:?}", e),
        }
    }

    /// Run a query against Influx and return the parsed results
    pub async fn query_data<T: FromMap>(&self, query: String) -> Result<Vec<T>> {
        let query = influxdb2::models::Query::new(query);
        match self.client.query::<T>(Some(query)).await {
            Ok(r) => Ok(r),
            Err(e) => anyhow::bail!("Encountered an error querying InfluxDB: '{:?}'", e),
        }
    }
}
