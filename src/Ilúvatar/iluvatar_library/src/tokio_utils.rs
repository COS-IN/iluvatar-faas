use crate::nproc;
use crate::transaction::TransactionId;
use crate::utils::missing_or_zero_default;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub type TokioRuntime = Arc<Runtime>;
/// All events in simulation need to happen on same runtime. Duplicate it and share under the hood.
static SIM_RUNTIME: Mutex<Option<TokioRuntime>> = Mutex::new(None);

pub fn build_tokio_runtime(
    tokio_event_interval: &Option<u32>,
    tokio_queue_interval: &Option<u32>,
    num_threads: &Option<usize>,
    tid: &TransactionId,
) -> anyhow::Result<TokioRuntime> {
    let is_sim = crate::utils::is_simulation();
    if is_sim {
        if let Some(rt) = SIM_RUNTIME.lock().as_ref() {
            return Ok(rt.clone());
        }
    }

    let num_threads = match num_threads {
        None => nproc(tid, false)? as usize,
        Some(n) => *n,
    };
    let event = missing_or_zero_default(tokio_event_interval, 61);
    let queue = missing_or_zero_default(tokio_queue_interval, 31);

    let mut rt = match is_sim {
        true => tokio::runtime::Builder::new_current_thread(),
        false => tokio::runtime::Builder::new_multi_thread(),
    };
    let rt = match rt
        .worker_threads(num_threads)
        .enable_all()
        .event_interval(event)
        .global_queue_interval(queue)
        .start_paused(is_sim) // Simulated runtime will always be paused
        .build()
    {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            anyhow::bail!(format!("Tokio thread runtime for main failed to start because: {}", e));
        }
    };
    if is_sim {
        SIM_RUNTIME.lock().replace(rt.clone());
    }
    Ok(rt)
}
