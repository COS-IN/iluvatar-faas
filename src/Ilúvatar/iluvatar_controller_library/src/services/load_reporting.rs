use iluvatar_library::transaction::{TransactionId, LOAD_MONITOR_TID};
use iluvatar_library::{
    influx::{InfluxClient, WORKERS_BUCKET},
    threading::tokio_thread,
};
use iluvatar_worker_library::{services::influx_updater::WorkerStatus, worker_api::worker_comm::WorkerAPIFactory};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

#[allow(unused)]
pub struct LoadService {
    _worker_thread: JoinHandle<()>,
    influx: Option<Arc<InfluxClient>>,
    workers: RwLock<HashMap<String, WorkerStatus>>,
    fact: Arc<WorkerAPIFactory>,
}

impl LoadService {
    pub fn boxed(
        influx: Option<Arc<InfluxClient>>,
        load_freq_ms: u64,
        _tid: &TransactionId,
        fact: Arc<WorkerAPIFactory>,
    ) -> anyhow::Result<Arc<Self>> {
        let (handle, tx) = tokio_thread(
            load_freq_ms,
            LOAD_MONITOR_TID.clone(),
            LoadService::monitor_worker_status,
        );
        if !iluvatar_library::utils::is_simulation() && influx.is_none() {
            anyhow::bail!("Cannot run a live system with no Influx set up!");
        }
        let ret = Arc::new(LoadService {
            _worker_thread: handle,
            workers: RwLock::new(HashMap::new()),
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
        // TODO: get new information (GPU, etc) from status in simulation
        let mut update = HashMap::new();
        let workers = self.fact.get_cached_workers();
        for (name, mut worker) in workers {
            let status: WorkerStatus = match worker.status(tid.to_string()).await {
                Ok(s) => s.into(),
                Err(e) => {
                    warn!(error=%e, tid=tid, "Unable to get status of simulation worker");
                    continue;
                },
            };
            update.insert(name, status);
        }

        info!(tid=tid, update=?update, "latest simulated worker update");
        *self.workers.write() = update;
    }

    #[tracing::instrument(skip(self), fields(tid=tid))]
    async fn monitor_live(&self, tid: &TransactionId) {
        let update = self.get_live_update(tid).await;
        info!(tid=tid, update=?update, "latest worker update");
        *self.workers.write() = update;
    }

    async fn get_live_update(&self, tid: &TransactionId) -> HashMap<String, WorkerStatus> {
        let query = format!(
            "from(bucket: \"{}\")
|> range(start: -1m)
|> last()",
            WORKERS_BUCKET,
        );
        let mut ret = HashMap::new();
        match &self.influx {
            Some(influx) => match influx.query_data::<WorkerStatus>(query).await {
                Ok(r) => {
                    for item in r {
                        debug!("{:?}", &item);
                        info!("{:?}", &item);
                        ret.insert(item.name.clone(), item);
                    }
                    if ret.is_empty() {
                        warn!(tid = tid, "Did not get any influx data in the last 5 minutes!")
                    }
                },
                Err(e) => error!(tid=tid, error=%e, "Failed to query worker status to InfluxDB"),
            },
            None => error!(tid = tid, "Influx client was not created during live run"),
        }
        ret
    }

    pub fn get_workers(&self) -> HashMap<String, WorkerStatus> {
        self.workers.read().clone()
    }
}
