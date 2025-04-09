use crate::services::load_balance::LoadMetric;
use iluvatar_library::transaction::{TransactionId, LOAD_MONITOR_TID};
use iluvatar_library::{
    influx::{InfluxClient, WORKERS_BUCKET},
    threading::tokio_thread,
};
use iluvatar_worker_library::{services::influx_updater::InfluxLoadData, worker_api::worker_comm::WorkerAPIFactory};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

#[allow(unused)]
pub struct LoadService {
    _worker_thread: JoinHandle<()>,
    influx: Option<Arc<InfluxClient>>,
    workers: RwLock<HashMap<String, f64>>,
    load_metric: String,
    fact: Arc<WorkerAPIFactory>,
}

impl LoadService {
    pub fn boxed(
        influx: Option<Arc<InfluxClient>>,
        load_metric: &LoadMetric,
        _tid: &TransactionId,
        fact: Arc<WorkerAPIFactory>,
    ) -> anyhow::Result<Arc<Self>> {
        let (handle, tx) = tokio_thread(
            load_metric.thread_sleep_ms,
            LOAD_MONITOR_TID.clone(),
            LoadService::monitor_worker_status,
        );
        if !iluvatar_library::utils::is_simulation() && influx.is_none() {
            anyhow::bail!("Cannot run a live system with no Influx set up!");
        }
        let ret = Arc::new(LoadService {
            _worker_thread: handle,
            workers: RwLock::new(HashMap::new()),
            load_metric: load_metric.load_metric.clone(),
            fact,
            influx,
        });
        tx.send(ret.clone())?;
        Ok(ret)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn monitor_worker_status(self: &Arc<Self>, tid: &TransactionId) {
        if iluvatar_library::utils::is_simulation() {
            self.monitor_simulation(tid).await;
        } else {
            self.monitor_live(tid).await;
        }
    }

    #[tracing::instrument(skip(self), fields(tid=tid))]
    async fn monitor_simulation(&self, tid: &TransactionId) {
        let mut update = HashMap::new();
        let workers = self.fact.get_cached_workers();
        for (name, mut worker) in workers {
            let status = match worker.status(tid.to_string()).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(error=%e, tid=tid, "Unable to get status of simulation worker");
                    continue;
                },
            };
            match self.load_metric.as_str() {
                "loadavg" => update.insert(
                    name,
                    (status.queue_len as f64 + status.num_running_funcs as f64) / status.num_system_cores as f64,
                ),
                "running" => update.insert(name, status.num_running_funcs as f64),
                "cpu_pct" => update.insert(name, status.num_running_funcs as f64 / status.num_system_cores as f64),
                "mem_pct" => update.insert(name, status.used_mem as f64 / status.total_mem as f64),
                "queue" => update.insert(name, status.queue_len as f64),
                _ => {
                    error!(tid=tid, metric=%self.load_metric, "Unknown load metric");
                    return;
                },
            };
        }

        info!(tid=tid, update=?update, "latest simulated worker update");
        *self.workers.write() = update;
    }

    #[tracing::instrument(skip(self), fields(tid=tid))]
    async fn monitor_live(&self, tid: &TransactionId) {
        let update = self.get_live_update(tid).await;
        let mut data = self.workers.read().clone();
        for (k, v) in update.iter() {
            data.insert(k.clone(), *v);
        }
        *self.workers.write() = data;

        info!(tid=tid, update=?update, "latest worker update");
    }

    async fn get_live_update(&self, tid: &TransactionId) -> HashMap<String, f64> {
        let query = format!(
            "from(bucket: \"{}\")
|> range(start: -5m)
|> filter(fn: (r) => r._measurement == \"{}\")
|> last()",
            WORKERS_BUCKET,
            self.load_metric.as_str()
        );
        let mut ret = HashMap::new();
        match &self.influx {
            Some(influx) => match influx.query_data::<InfluxLoadData>(query).await {
                Ok(r) => {
                    for item in r {
                        debug!("{:?}", &item);
                        ret.insert(item.name, item.value);
                    }
                    if ret.is_empty() {
                        warn!(
                            tid = tid,
                            "Did not get any data in the last 5 minutes using the load metric '{}'",
                            self.load_metric.as_str()
                        );
                    }
                },
                Err(e) => error!(tid=tid, error=%e, "Failed to query worker status to InfluxDB"),
            },
            None => error!(tid = tid, "Influx client was not created during live run"),
        }
        ret
    }

    pub fn get_worker(&self, name: &str) -> Option<f64> {
        self.workers.read().get(name).copied()
    }

    pub fn get_workers(&self) -> HashMap<String, f64> {
        self.workers.read().clone()
    }
}
