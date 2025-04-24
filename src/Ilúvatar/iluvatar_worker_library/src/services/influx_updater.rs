use crate::services::containers::containermanager::{ContainerMgrStat, CTR_MGR_WORKER_TID};
use crate::services::invocation::dispatching::queueing_dispatcher::DISPATCHER_INVOKER_LOG_TID;
use crate::services::invocation::InvokerLoad;
use crate::services::resources::cpu::{CpuUtil, CPU_MON_TID};
use anyhow::Result;
use iluvatar_library::clock::{get_global_clock, Clock};
use iluvatar_library::influx::{InfluxClient, InfluxConfig, WORKERS_BUCKET};
use iluvatar_library::ring_buff::RingBuffer;
use iluvatar_library::types::Compute;
use iluvatar_library::{threading::tokio_thread, transaction::TransactionId};
use iluvatar_rpc::rpc::StatusResponse;
use influxdb2::FromDataPoint;
use influxdb2_derive::WriteDataPoint;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
// use crate::services::resources::gpu::{GpuStatVec, GPU_RESC_TID};

#[derive(FromDataPoint, WriteDataPoint, Clone, PartialEq, Default, Debug)]
#[measurement = "worker_load"]
pub struct WorkerStatus {
    #[influxdb(field)]
    pub cpu_loadavg: f64,
    #[influxdb(field)]
    pub gpu_loadavg: f64,
    #[influxdb(field)]
    pub cpu_util: f64,
    #[influxdb(field)]
    pub cpu_queue_len: u64,
    #[influxdb(field)]
    pub gpu_queue_len: u64,
    #[influxdb(field)]
    pub host_mem_pct: f64,
    #[influxdb(field)]
    pub num_running_funcs: u64,
    #[influxdb(tag)]
    pub name: String,
    #[influxdb(tag)]
    pub node_type: String,
    #[influxdb(timestamp)]
    time: i64,
}
impl From<StatusResponse> for WorkerStatus {
    fn from(value: StatusResponse) -> Self {
        WorkerStatus {
            cpu_loadavg: value.cpu_load_avg,
            gpu_loadavg: value.cpu_load_avg,
            cpu_util: value.cpu_util,
            cpu_queue_len: value.cpu_queue_len,
            gpu_queue_len: value.gpu_queue_len,
            host_mem_pct: value.used_mem_pct,
            num_running_funcs: value.num_running_funcs as u64,
            name: "".to_string(),
            node_type: "".to_string(),
            time: 0,
        }
    }
}

/// A struct to regularly send data to InfluxDb, if being used
pub struct InfluxUpdater {
    influx: Arc<InfluxClient>,
    ring_buff: Arc<RingBuffer>,
    _status_handle: JoinHandle<()>,
    worker_name: String,
    clock: Clock,
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
                worker_name,
                clock: get_global_clock(tid)?,
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
        let (cpu_loadavg, cpu_util) = self.ring_buff.latest(CPU_MON_TID).map_or((0.0, 0.0), |cpu| {
            match iluvatar_library::downcast!(cpu.1, CpuUtil) {
                None => (0.0, 0.0),
                Some(cpu) => (cpu.load_avg_1minute, 100.0 - cpu.cpu_id),
            }
        });
        // let (used_mem, total_mem) = self.ring_buff.latest(&GPU_RESC_TID).map_or((0, 0), |gpu| {
        //     match iluvatar_library::downcast!(gpu.1, GpuStatVec) {
        //         None => (0, 0),
        //         Some(que) => (que.used_mem, que.total_mem),
        //     }
        // });
        let (running, cpu_len, (gpu_len, gpu_loadavg)) =
            self.ring_buff
                .latest(DISPATCHER_INVOKER_LOG_TID)
                .map_or((0, 0, (0, 0.0)), |que| {
                    match iluvatar_library::downcast!(que.1, InvokerLoad) {
                        None => (0, 0, (0, 0.0)),
                        Some(que) => (
                            que.num_running_funcs,
                            que.queues.get(&Compute::CPU).map_or(0, |q| q.len) as u64,
                            que.queues
                                .get(&Compute::GPU)
                                .map_or((0, 0.0), |q| (q.len as u64, q.load_avg)),
                        ),
                    }
                });
        let (used_mem, total_mem) = self.ring_buff.latest(&CTR_MGR_WORKER_TID).map_or((0, 0), |que| {
            match iluvatar_library::downcast!(que.1, ContainerMgrStat) {
                None => (0, 0),
                Some(que) => (que.used_mem, que.total_mem),
            }
        });
        let data = vec![WorkerStatus {
            cpu_loadavg,
            gpu_loadavg,
            cpu_util,
            cpu_queue_len: cpu_len,
            gpu_queue_len: gpu_len,
            host_mem_pct: used_mem as f64 / total_mem as f64,
            name: self.worker_name.clone(),
            num_running_funcs: running as u64,
            node_type: "worker".to_string(),
            time: self.clock.now().unix_timestamp_nanos() as i64,
        }];
        debug!(tid = tid, "writing to influx");
        match self.influx.write_struct_data(WORKERS_BUCKET, data).await {
            Ok(_) => (),
            Err(e) => error!(tid=tid, error=%e, "Failed to write worker status to InfluxDB"),
        };
    }
}
