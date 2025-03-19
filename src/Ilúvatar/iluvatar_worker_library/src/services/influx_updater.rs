use crate::services::containers::containermanager::{ContainerMgrStat, CTR_MGR_WORKER_TID};
use crate::services::invocation::dispatching::queueing_dispatcher::DISPATCHER_INVOKER_LOG_TID;
use crate::services::invocation::InvokerLoad;
use crate::services::resources::cpu::{CpuUtil, CPU_MON_TID};
use anyhow::Result;
use iluvatar_library::influx::{InfluxClient, InfluxConfig, WORKERS_BUCKET};
use iluvatar_library::ring_buff::RingBuffer;
use iluvatar_library::types::Compute;
use iluvatar_library::{threading::tokio_thread, transaction::TransactionId};
use influxdb2::FromDataPoint;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

#[derive(Debug, FromDataPoint, Default)]
pub struct InfluxLoadData {
    pub value: f64,
    pub name: String,
    pub node_type: String,
}

/// A struct to regularly send data to InfluxDb, if being used
pub struct InfluxUpdater {
    influx: Arc<InfluxClient>,
    ring_buff: Arc<RingBuffer>,
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
        ring_buff: &Arc<RingBuffer>,
        worker_name: String,
        tid: &TransactionId,
    ) -> Result<Option<Arc<Self>>> {
        if let Some(influx) = influx {
            info!(tid = tid, "Building InfluxUpdater");
            let (stat, stat_tx) = tokio_thread(config.update_freq_ms, INFLUX_STATUS_TID.clone(), Self::send_status);

            let r = Arc::new(Self {
                _status_handle: stat,
                tags: format!("name={},node_type=worker", worker_name),
                metrics: vec!["loadavg,", "cpu_util,", "queue_len,", "mem_pct,", "used_mem,"],
                influx,
                ring_buff: ring_buff.clone(),
            });
            stat_tx.send(r.clone())?;
            return Ok(Some(r));
        }
        info!(tid = tid, "Building InfluxUpdater");

        Ok(None)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn send_status(self: &Arc<Self>, tid: &TransactionId) {
        let (load_avg_1minute, cpu_id) = self.ring_buff.latest(CPU_MON_TID).map_or((0.0, 0.0), |cpu| {
            match iluvatar_library::downcast!(cpu.1, CpuUtil) {
                None => (0.0, 0.0),
                Some(cpu) => (cpu.load_avg_1minute, cpu.cpu_id),
            }
        });
        let queue_len = self.ring_buff.latest(DISPATCHER_INVOKER_LOG_TID).map_or(0, |que| {
            match iluvatar_library::downcast!(que.1, InvokerLoad) {
                None => 0,
                Some(que) => {
                    que.0.get(&Compute::CPU).map_or(0, |q| q.len) + que.0.get(&Compute::GPU).map_or(0, |q| q.len)
                },
            }
        }) as f64;
        let (used_mem, total_mem) = self.ring_buff.latest(&CTR_MGR_WORKER_TID).map_or((0, 0), |que| {
            match iluvatar_library::downcast!(que.1, ContainerMgrStat) {
                None => (0, 0),
                Some(que) => (que.used_mem, que.total_mem),
            }
        });
        let mut builder = String::new();
        for measure in &self.metrics {
            let val = match *measure {
                "loadavg," => load_avg_1minute,
                "cpu_util," => 100.0 - cpu_id,
                "queue_len," => queue_len,
                "mem_pct," => (used_mem as f64 / total_mem as f64) * 100.0,
                "used_mem," => used_mem as f64,
                _ => continue,
            };
            if val.is_nan() {
                continue;
            }
            builder.push_str(measure);
            builder.push_str(&self.tags);
            builder.push_str(&format!(" value={}\n", val));
        }
        debug!(tid = tid, data = builder, "writing to influx");
        match self.influx.write_data(WORKERS_BUCKET, builder).await {
            Ok(_) => (),
            Err(e) => error!(tid=tid, error=%e, "Failed to write worker status to InfluxDB"),
        };
    }
}
