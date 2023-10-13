use super::status::StatusService;
use anyhow::Result;
use iluvatar_library::influx::{InfluxClient, InfluxConfig, WORKERS_BUCKET};
use iluvatar_library::{threading::tokio_thread, transaction::TransactionId};
use influxdb2::FromDataPoint;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug, FromDataPoint, Default)]
pub struct InfluxLoadData {
    pub value: f64,
    pub name: String,
    pub node_type: String,
}

/// A struct to regularly send data to InfluxDb, if being used
pub struct InfluxUpdater {
    influx: Arc<InfluxClient>,
    status_svc: Arc<StatusService>,
    _status_handle: JoinHandle<()>,
    tags: String,
    metrics: Vec<&'static str>,
}

lazy_static::lazy_static! {
  pub static ref INFLUX_STATUS_TID: TransactionId = "StatusSenderInflux".to_string();
}
impl InfluxUpdater {
    pub fn boxed(
        influx: Option<Arc<InfluxClient>>,
        config: Arc<InfluxConfig>,
        status_svc: Arc<StatusService>,
        worker_name: String,
        tid: &TransactionId,
    ) -> Result<Option<Arc<Self>>> {
        if let Some(influx) = influx {
            info!(tid=%tid, "Building InfluxUpdater");
            let (stat, stat_tx) = tokio_thread(config.update_freq_ms, INFLUX_STATUS_TID.clone(), Self::send_status);

            let r = Arc::new(Self {
                _status_handle: stat,
                tags: format!("name={},node_type=worker", worker_name),
                metrics: vec!["loadavg,", "cpu_util,", "queue_len,", "mem_pct,", "used_mem,"],
                influx,
                status_svc,
            });
            stat_tx.send(r.clone())?;
            return Ok(Some(r));
        }
        info!(tid=%tid, "Building InfluxUpdater");

        Ok(None)
    }

    async fn send_status(svc: Arc<Self>, tid: TransactionId) {
        let status = svc.status_svc.get_status(&tid);
        let mut builder = String::new();
        for measure in &svc.metrics {
            let val = match measure {
                &"loadavg," => status.load_avg_1minute,
                &"cpu_util," => 100.0 - status.cpu_id,
                &"queue_len," => (status.cpu_queue_len + status.gpu_queue_len) as f64,
                &"mem_pct," => (status.used_mem as f64 / status.total_mem as f64) * 100.0,
                &"used_mem," => status.used_mem as f64,
                _ => continue,
            };
            if val.is_nan() {
                continue;
            }
            builder.push_str(measure);
            builder.push_str(&svc.tags);
            builder.push_str(&format!(" value={}\n", val));
        }
        info!(tid=%tid, data=builder, "writing to influx");
        match svc.influx.write_data(WORKERS_BUCKET, builder).await {
            Ok(_) => (),
            Err(e) => error!(tid=%tid, error=%e, "Failed to write worker status to InfluxDB"),
        };
    }
}
