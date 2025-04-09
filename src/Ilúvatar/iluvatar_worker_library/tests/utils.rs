use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::{
    clock::ContainerTimeFormatter,
    logging::{start_tracing, LoggingConfig},
    transaction::{TransactionId, SIMULATION_START_TID, TEST_TID},
    types::{Compute, Isolation, MemSizeMb},
};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::{
    containers::{containermanager::ContainerManager, simulator::simstructs::SimulationInvocation},
    invocation::{InvocationResult, Invoker},
    registration::{RegisteredFunction, RegistrationService},
    resources::gpu::GpuResourceTracker,
};
use iluvatar_worker_library::worker_api::config::{WorkerConfig, WORKER_ENV_PREFIX};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use time::OffsetDateTime;
use tokio::{task::JoinHandle, time::timeout};

#[macro_export]
macro_rules! assert_error {
    ($err:expr, $exp:expr, $noerr:expr) => {
        match $err {
            Ok(_) => panic!("{}", $noerr),
            Err(e) => {
                assert_eq!(e.to_string(), $exp);
            },
        };
    };
}

/// Creates/sets up the services needed to test a worker setup.
/// Passing [log] = Some("<level>") will enable logging to stdout, useful for test debugging.
/// [config_pth] is an optional path to config to load.
pub async fn sim_test_services(
    config_pth: Option<&str>,
    overrides: Option<Vec<(String, String)>>,
    log: Option<&str>,
) -> (
    Option<impl Drop>,
    WorkerConfig,
    Arc<ContainerManager>,
    Arc<dyn Invoker>,
    Arc<RegistrationService>,
    WorkerCharMap,
    Option<Arc<GpuResourceTracker>>,
) {
    iluvatar_library::utils::set_simulation(&SIMULATION_START_TID).unwrap();
    build_test_services(config_pth, overrides, log, &SIMULATION_START_TID).await
}

/// Creates/sets up the services needed to test a worker setup.
/// Passing [log] = Some("<level>") will enable logging to stdout, useful for test debugging.
/// [config_pth] is an optional path to config to load.
pub async fn test_invoker_svc(
    config_pth: Option<&str>,
    overrides: Option<Vec<(String, String)>>,
    log: Option<&str>,
) -> (
    Option<impl Drop>,
    WorkerConfig,
    Arc<ContainerManager>,
    Arc<dyn Invoker>,
    Arc<RegistrationService>,
    WorkerCharMap,
    Option<Arc<GpuResourceTracker>>,
) {
    build_test_services(config_pth, overrides, log, &TEST_TID).await
}

async fn build_test_services(
    config_pth: Option<&str>,
    overrides: Option<Vec<(String, String)>>,
    log: Option<&str>,
    tid: &TransactionId,
) -> (
    Option<impl Drop>,
    WorkerConfig,
    Arc<ContainerManager>,
    Arc<dyn Invoker>,
    Arc<RegistrationService>,
    WorkerCharMap,
    Option<Arc<GpuResourceTracker>>,
) {
    let cfg: WorkerConfig = iluvatar_library::load_config_default!(
        "iluvatar_worker_library/tests/resources/worker.json",
        config_pth,
        overrides,
        WORKER_ENV_PREFIX
    )
    .unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
    let log = match log {
        Some(level) => {
            let fake_logging = Arc::new(LoggingConfig {
                level: level.to_string(),
                spanning: cfg.logging.spanning.clone(),
                directory: "/tmp".to_string(),
                basename: "test".to_string(),
                stdout: Some(true),
                ..std::default::Default::default()
            });
            Some(
                start_tracing(&fake_logging, &TEST_TID)
                    .unwrap_or_else(|e| panic!("Failed to load start tracing for test: {}", e)),
            )
        },
        None => None,
    };

    let worker = iluvatar_worker_library::worker_api::create_worker(cfg.clone(), tid)
        .await
        .unwrap_or_else(|e| panic!("Error creating worker: {}", e));
    (
        log,
        cfg,
        worker.container_manager,
        worker.invoker,
        worker.reg,
        worker.cmap,
        worker.gpu,
    )
}

fn basic_reg_req(image: &str, name: &str) -> RegisterRequest {
    RegisterRequest {
        function_name: name.to_string(),
        function_version: "0.1.1".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: image.to_string(),
        transaction_id: "testTID".to_string(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        ..std::default::Default::default()
    }
}

pub async fn cust_register(
    reg: &Arc<RegistrationService>,
    image: &str,
    name: &str,
    memory: MemSizeMb,
    tid: &TransactionId,
) -> Arc<RegisteredFunction> {
    let req = RegisterRequest {
        function_name: name.to_string(),
        function_version: "0.1.1".to_string(),
        cpus: 1,
        memory,
        parallel_invokes: 1,
        image_name: image.to_string(),
        transaction_id: "testTID".to_string(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        ..std::default::Default::default()
    };
    register_internal(reg, req, tid).await
}

pub async fn register(
    reg: &Arc<RegistrationService>,
    image: &str,
    name: &str,
    tid: &TransactionId,
) -> Arc<RegisteredFunction> {
    register_internal(reg, basic_reg_req(image, name), tid).await
}

async fn register_internal(
    reg: &Arc<RegistrationService>,
    req: RegisterRequest,
    tid: &TransactionId,
) -> Arc<RegisteredFunction> {
    timeout(Duration::from_secs(20), reg.register(req, tid))
        .await
        .unwrap_or_else(|e| panic!("register timout hit: {:?}", e))
        .unwrap_or_else(|e| panic!("register failed: {:?}", e))
}

pub type HANDLE = JoinHandle<Result<Arc<Mutex<InvocationResult>>, anyhow::Error>>;
pub fn background_test_invoke(
    invok_svc: &Arc<dyn Invoker>,
    reg: &Arc<RegisteredFunction>,
    json_args: &str,
    transaction_id: &TransactionId,
) -> HANDLE {
    let cln = invok_svc.clone();
    let j = json_args.to_string();
    let t = transaction_id.clone();
    let r = reg.clone();
    tokio::spawn(async move { cln.sync_invocation(r, j, t).await })
}

pub async fn wait_for_queue_len(invok_svc: &Arc<dyn Invoker>, compute_queue: Compute, len: usize) {
    timeout(Duration::from_secs(20), async move {
        loop {
            if invok_svc.queue_len().0.get(&compute_queue).unwrap().len >= len {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Timeout error waiting for queue length to reach length {}", len));
}

pub async fn get_start_end_time_from_invoke(
    handle: HANDLE,
    formatter: &ContainerTimeFormatter,
) -> (OffsetDateTime, OffsetDateTime) {
    let result = resolve_invoke(handle).await;
    match result {
        Ok(result_ptr) => {
            let result = result_ptr.lock();
            let worker_result = result
                .worker_result
                .as_ref()
                .unwrap_or_else(|| panic!("worker_result should have been set on '{:?}'", result));
            let parsed_start = formatter
                .parse_python_container_time(&worker_result.start)
                .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.start, e));
            let parsed_end = formatter
                .parse_python_container_time(&worker_result.end)
                .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.end, e));
            assert!(parsed_start < parsed_end, "Start and end times cannot be inversed!");
            assert!(result.duration.as_micros() > 0, "Duration should not be <= 0!");
            assert_ne!(result.result_json, "", "result_json should not be empty!");
            (parsed_start, parsed_end)
        },
        Err(e) => panic!("Invocation failed: {}", e),
    }
}

pub async fn resolve_invoke(handle: HANDLE) -> anyhow::Result<Arc<Mutex<InvocationResult>>> {
    timeout(Duration::from_secs(20), handle)
        .await
        .unwrap_or_else(|e| panic!("Timeout error on invocation: {:?}", e))
        .unwrap_or_else(|e| panic!("Error joining invocation thread handle: {:?}", e))
}

pub async fn test_invoke(
    invok_svc: &Arc<dyn Invoker>,
    reg: &Arc<RegisteredFunction>,
    json_args: &str,
    tid: &TransactionId,
) -> Arc<Mutex<InvocationResult>> {
    let result = timeout(
        Duration::from_secs(20),
        background_test_invoke(invok_svc, reg, json_args, tid),
    )
    .await
    .unwrap_or_else(|e| panic!("invoke timout hit: {:?}", e))
    .unwrap_or_else(|e| panic!("invoke tokio join error: {:?}", e))
    .unwrap_or_else(|e| panic!("invoke failed: {:?}", e));
    if result.lock().worker_result.is_none() {
        panic!("Invocation completed without a result! {:?}", result);
    }
    result
}

pub async fn prewarm(cm: &Arc<ContainerManager>, reg: &Arc<RegisteredFunction>, transaction_id: &TransactionId) {
    timeout(Duration::from_secs(20), cm.prewarm(reg, transaction_id, Compute::CPU))
        .await
        .unwrap_or_else(|e| panic!("prewarm timout hit: {:?}", e))
        .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
}

pub fn sim_args() -> anyhow::Result<String> {
    Ok(serde_json::to_string(&SimulationInvocation::from([
        (
            Compute::CPU,
            iluvatar_worker_library::services::containers::simulator::simstructs::SimInvokeData {
                warm_dur_ms: 1000,
                cold_dur_ms: 5000,
            },
        ),
        (
            Compute::GPU,
            iluvatar_worker_library::services::containers::simulator::simstructs::SimInvokeData {
                warm_dur_ms: 1000,
                cold_dur_ms: 5000,
            },
        ),
    ]))?)
}

pub fn short_sim_args() -> anyhow::Result<String> {
    Ok(serde_json::to_string(&SimulationInvocation::from([
        (
            Compute::CPU,
            iluvatar_worker_library::services::containers::simulator::simstructs::SimInvokeData {
                warm_dur_ms: 500,
                cold_dur_ms: 1000,
            },
        ),
        (
            Compute::GPU,
            iluvatar_worker_library::services::containers::simulator::simstructs::SimInvokeData {
                warm_dur_ms: 500,
                cold_dur_ms: 1000,
            },
        ),
    ]))?)
}
