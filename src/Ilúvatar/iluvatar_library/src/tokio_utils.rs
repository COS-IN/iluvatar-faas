use crate::nproc;
use crate::transaction::TransactionId;
use crate::utils::missing_or_zero_default;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;

pub type TokioRuntime = Arc<Runtime>;
/// All events in simulation need to happen on same runtime. Duplicate it and share under the hood.
static SIM_RUNTIME: Mutex<Option<TokioRuntime>> = Mutex::new(None);

/// Calculate the duration of the given number of simulation ticks
pub fn compute_sim_tick_dur(sim_ticks: u64, granularity: SimulationGranularity) -> Duration {
    match granularity {
        SimulationGranularity::S => Duration::from_secs(sim_ticks),
        SimulationGranularity::MS => Duration::from_millis(sim_ticks),
        SimulationGranularity::US => Duration::from_micros(sim_ticks),
        SimulationGranularity::NS => Duration::from_nanos(sim_ticks),
    }
}

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
#[warn(clippy::disallowed_methods)]
pub async fn sim_scheduler_tick(tick_step: u64, granularity: SimulationGranularity) {
    // TODO: Tokio sleep as millisecond granularity, limiting possible precision of simulation
    // https://github.com/tokio-rs/tokio/discussions/5996
    // Try: https://docs.rs/tokio-timerfd/latest/tokio_timerfd/struct.Delay.html
    tokio::time::sleep(compute_sim_tick_dur(tick_step, granularity)).await;
}

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
        // Only one runtime un sim, clone to share if anyone creates another runtime in future
        SIM_RUNTIME.lock().replace(rt.clone());
    }
    Ok(rt)
}
