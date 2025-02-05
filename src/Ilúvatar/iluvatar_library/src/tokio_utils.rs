use crate::transaction::TransactionId;
use crate::utils::missing_or_zero_default;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

pub type TokioRuntime = Arc<Runtime>;
/// All events in simulation need to happen on same runtime. Duplicate it and share under the hood.
static SIM_RUNTIME: Mutex<Option<TokioRuntime>> = Mutex::new(None);

/// Time granularity for simulation to run at
#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum SimulationGranularity {
    /// Second
    S,
    /// Millisecond
    MS,
    /// Microsecond
    US,
    /// Nanosecond
    NS,
}

/// Increment the simulation one tick.
/// Yields back to the Tokio scheduler to allow outstanding tasks to be polled.
pub async fn sim_scheduler_tick(tick_step: u64, granularity: SimulationGranularity) {
    match granularity {
        SimulationGranularity::S => tokio::time::advance(Duration::from_secs(tick_step)).await,
        SimulationGranularity::MS => tokio::time::advance(Duration::from_millis(tick_step)).await,
        SimulationGranularity::US => tokio::time::advance(Duration::from_micros(tick_step)).await,
        SimulationGranularity::NS => tokio::time::advance(Duration::from_nanos(tick_step)).await,
    }
}

pub fn build_tokio_runtime(
    tokio_event_interval: &Option<u32>,
    tokio_queue_interval: &Option<u32>,
    num_threads: &Option<usize>,
    _tid: &TransactionId,
) -> anyhow::Result<TokioRuntime> {
    let is_sim = crate::utils::is_simulation();
    if is_sim {
        if let Some(rt) = SIM_RUNTIME.lock().as_ref() {
            return Ok(rt.clone());
        }
    }

    let event = missing_or_zero_default(tokio_event_interval, 61);
    let queue = missing_or_zero_default(tokio_queue_interval, 31);

    let mut rt = match is_sim {
        true => tokio::runtime::Builder::new_current_thread(),
        false => tokio::runtime::Builder::new_multi_thread(),
    };
    let rt: &mut tokio::runtime::Builder = match num_threads {
        Some(n) => rt.worker_threads(*n),
        None => &mut rt,
    };
    let rt = match rt
        .enable_all()
        .event_interval(event)
        .global_queue_interval(queue)
        .start_paused(is_sim) // Simulated runtime will always be paused
        .build()
    {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            anyhow::bail!(format!("Tokio thread runtime for main failed to start because: {}", e));
        },
    };
    if is_sim {
        // Only one runtime un sim, clone to share if anyone creates another runtime in future
        SIM_RUNTIME.lock().replace(rt.clone());
    }
    Ok(rt)
}
