use iluvatar_library::clock::ContainerTimeFormatter;
#[cfg(feature = "power_cap")]
use iluvatar_library::energy::energy_logging::EnergyLogger;
use iluvatar_library::types::{Compute, Isolation, MemSizeMb};
use iluvatar_library::{
    characteristics_map::{AgExponential, CharacteristicsMap},
    logging::{start_tracing, LoggingConfig},
    transaction::{TransactionId, TEST_TID},
};
use iluvatar_rpc::rpc::{LanguageRuntime, RegisterRequest};
use iluvatar_worker_library::services::containers::simulator::simstructs::SimulationInvocation;
#[cfg(feature = "power_cap")]
use iluvatar_worker_library::services::invocation::energy_limiter::EnergyLimiter;
use iluvatar_worker_library::services::status::status_service::build_load_avg_signal;
use iluvatar_worker_library::services::{
    containers::{containermanager::ContainerManager, IsolationFactory},
    invocation::{Invoker, InvokerFactory},
};
use iluvatar_worker_library::services::{
    invocation::InvocationResult,
    resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker},
};
use iluvatar_worker_library::{
    services::registration::{RegisteredFunction, RegistrationService},
    worker_api::config::{Configuration, WorkerConfig},
};
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

/// Creates/sets up the structs needed to test an invoker setup
/// The [env] will set process-level environment vars to adjust config. Clean these up with [clean_env]
/// Passing [log] = Some(true)  will enable logging to stdout. Only pass this on one test as it will be set globally and can only be done once
/// [config_pth] is an optional path to config to load
pub async fn full_sim_invoker(
    config_pth: Option<String>,
    overrides: Option<Vec<(String, String)>>,
    log: Option<bool>,
) -> (
    Option<impl Drop>,
    WorkerConfig,
    Arc<ContainerManager>,
    Arc<dyn Invoker>,
    Arc<RegistrationService>,
    Arc<CharacteristicsMap>,
    Option<Arc<GpuResourceTracker>>,
    Arc<CpuResourceTracker>,
) {
    let tid: &TransactionId = &iluvatar_library::transaction::SIMULATION_START_TID;
    iluvatar_library::utils::file::ensure_temp_dir().unwrap();
    iluvatar_library::utils::set_simulation(tid).unwrap();
    let log = log.unwrap_or(false);
    let worker_name = "TEST".to_string();
    let test_cfg_pth = config_pth.unwrap_or_else(|| "tests/resources/worker.dev.json".to_string());
    let cfg = Configuration::boxed(&Some(&test_cfg_pth), overrides)
        .unwrap_or_else(|e| panic!("Failed to load config file for sim test: {:?}", e));
    let fake_logging = Arc::new(LoggingConfig {
        level: cfg.logging.level.clone(),
        spanning: cfg.logging.spanning.clone(),
        ..Default::default()
    });
    let _log = match log {
        true => Some(
            start_tracing(fake_logging, &worker_name, &TEST_TID)
                .unwrap_or_else(|e| panic!("Failed to load start tracing for test: {}", e)),
        ),
        false => None,
    };
    let load_avg = build_load_avg_signal();
    let cmap = Arc::new(CharacteristicsMap::new(AgExponential::new(0.6)));
    let cpu = CpuResourceTracker::new(&cfg.container_resources.cpu_resource, load_avg.clone(), &TEST_TID)
        .unwrap_or_else(|e| panic!("Failed to create cpu resource man: {}", e));
    let factory = IsolationFactory::new(cfg.clone());
    let lifecycles = factory
        .get_isolation_services(&TEST_TID, false)
        .await
        .unwrap_or_else(|e| panic!("Failed to create lifecycle: {}", e));
    let gpu_resource = GpuResourceTracker::boxed(
        &cfg.container_resources.gpu_resource,
        &cfg.container_resources,
        &TEST_TID,
        &lifecycles.get(&Isolation::DOCKER),
        &cfg.status,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to create gpu resource man: {}", e));

    let cm = ContainerManager::boxed(
        cfg.container_resources.clone(),
        lifecycles.clone(),
        gpu_resource.clone(),
        &TEST_TID,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to create container manger for test: {}", e));
    let reg = RegistrationService::new(
        cm.clone(),
        lifecycles.clone(),
        cfg.limits.clone(),
        cmap.clone(),
        cfg.container_resources.clone(),
    );
    #[cfg(feature = "power_cap")]
    let en_log = EnergyLogger::boxed(None, &TEST_TID)
        .await
        .unwrap_or_else(|e| panic!("Failed to create energy logger: {}", e));
    #[cfg(feature = "power_cap")]
    let energy =
        EnergyLimiter::boxed(&None, en_log).unwrap_or_else(|e| panic!("Failed to create energy limiter: {}", e));
    let invoker_fact = InvokerFactory::new(
        cm.clone(),
        cfg.limits.clone(),
        cfg.invocation.clone(),
        cmap.clone(),
        cpu.clone(),
        gpu_resource.clone(),
        cfg.container_resources.gpu_resource.clone(),
        &reg,
        #[cfg(feature = "power_cap")]
        energy,
    );
    let invoker = invoker_fact
        .get_invoker_service(&TEST_TID)
        .unwrap_or_else(|e| panic!("Failed to create invoker service because: {}", e));
    (_log, cfg, cm, invoker, reg, cmap, gpu_resource, cpu)
}

/// Creates/sets up the structs needed to test an invoker setup
/// The [env] will set process-level environment vars to adjust config. Clean these up with [clean_env]
/// Passing `log_level` = Some('info')  will enable logging to stdout at the specified level. Only pass this on one test as it will be set globally and can only be done once
/// `config_pth` is an optional path to config to load
pub async fn sim_invoker_svc(
    config_pth: Option<String>,
    overrides: Option<Vec<(String, String)>>,
    log_level: Option<&str>,
) -> (
    Option<impl Drop>,
    WorkerConfig,
    Arc<ContainerManager>,
    Arc<dyn Invoker>,
    Arc<RegistrationService>,
    Arc<CharacteristicsMap>,
) {
    let tid: &TransactionId = &iluvatar_library::transaction::SIMULATION_START_TID;
    iluvatar_library::utils::file::ensure_temp_dir().unwrap();
    iluvatar_library::utils::set_simulation(tid).unwrap();
    let worker_name = "TEST".to_string();
    let test_cfg_pth = config_pth.unwrap_or_else(|| "tests/resources/worker.dev.json".to_string());
    let cfg = Configuration::boxed(&Some(&test_cfg_pth), overrides)
        .unwrap_or_else(|e| panic!("Failed to load config file for sim test: {:?}", e));
    let _log = match log_level {
        Some(log_level) => {
            let mut fake_logging = (*cfg.logging).clone();
            fake_logging.stdout = Some(true);
            fake_logging.level = log_level.to_string();
            let fake_logging = Arc::new(fake_logging);
            Some(
                start_tracing(fake_logging, &worker_name, &TEST_TID)
                    .unwrap_or_else(|e| panic!("Failed to load start tracing for test: {}", e)),
            )
        },
        None => None,
    };
    let load_avg = build_load_avg_signal();
    let cmap = Arc::new(CharacteristicsMap::new(AgExponential::new(0.6)));
    let cpu = CpuResourceTracker::new(&cfg.container_resources.cpu_resource, load_avg, &TEST_TID)
        .unwrap_or_else(|e| panic!("Failed to create cpu resource man: {}", e));
    let factory = IsolationFactory::new(cfg.clone());
    let lifecycles = factory
        .get_isolation_services(&TEST_TID, false)
        .await
        .unwrap_or_else(|e| panic!("Failed to create lifecycle: {}", e));
    let gpu_resource = GpuResourceTracker::boxed(
        &cfg.container_resources.gpu_resource,
        &cfg.container_resources,
        &TEST_TID,
        &lifecycles.get(&Isolation::DOCKER),
        &cfg.status,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to create gpu resource man: {}", e));

    let cm = ContainerManager::boxed(
        cfg.container_resources.clone(),
        lifecycles.clone(),
        gpu_resource.clone(),
        &TEST_TID,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to create container manger for test: {}", e));
    let reg = RegistrationService::new(
        cm.clone(),
        lifecycles.clone(),
        cfg.limits.clone(),
        cmap.clone(),
        cfg.container_resources.clone(),
    );
    #[cfg(feature = "power_cap")]
    let en_log = EnergyLogger::boxed(None, &TEST_TID)
        .await
        .unwrap_or_else(|e| panic!("Failed to create energy logger: {}", e));
    #[cfg(feature = "power_cap")]
    let energy =
        EnergyLimiter::boxed(&None, en_log).unwrap_or_else(|e| panic!("Failed to create energy limiter: {}", e));
    let invoker_fact = InvokerFactory::new(
        cm.clone(),
        cfg.limits.clone(),
        cfg.invocation.clone(),
        cmap.clone(),
        cpu,
        gpu_resource,
        cfg.container_resources.gpu_resource.clone(),
        &reg,
        #[cfg(feature = "power_cap")]
        energy,
    );
    let invoker = invoker_fact
        .get_invoker_service(&TEST_TID)
        .unwrap_or_else(|e| panic!("Failed to create invoker service because: {}", e));
    (_log, cfg, cm, invoker, reg, cmap)
}

/// Creates/sets up the structs needed to test an invoker setup
/// The [env] will set process-level environment vars to adjust config. Clean these up with [clean_env]
/// Passing [log] = Some(true)  will enable logging to stdout. Only pass this on one test as it will be set globally and can only be done once
/// [config_pth] is an optional path to config to load
pub async fn test_invoker_svc(
    config_pth: Option<String>,
    overrides: Option<Vec<(String, String)>>,
    log: Option<bool>,
) -> (
    Option<impl Drop>,
    WorkerConfig,
    Arc<ContainerManager>,
    Arc<dyn Invoker>,
    Arc<RegistrationService>,
) {
    iluvatar_library::utils::file::ensure_temp_dir().unwrap();
    let log = log.unwrap_or(false);
    let worker_name = "TEST".to_string();
    let test_cfg_pth = config_pth.unwrap_or_else(|| "tests/resources/worker.dev.json".to_string());
    let cfg = Configuration::boxed(&Some(&test_cfg_pth), overrides)
        .unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
    let fake_logging = Arc::new(LoggingConfig {
        level: cfg.logging.level.clone(),
        spanning: cfg.logging.spanning.clone(),
        directory: "/tmp".to_string(),
        basename: "test".to_string(),
        ..Default::default()
    });
    let _log = match log {
        true => Some(
            start_tracing(fake_logging, &worker_name, &TEST_TID)
                .unwrap_or_else(|e| panic!("Failed to load start tracing for test: {}", e)),
        ),
        false => None,
    };
    let load_avg = build_load_avg_signal();
    let cmap = Arc::new(CharacteristicsMap::new(AgExponential::new(0.6)));
    let cpu = CpuResourceTracker::new(&cfg.container_resources.cpu_resource, load_avg, &TEST_TID)
        .unwrap_or_else(|e| panic!("Failed to create cpu resource man: {}", e));

    let factory = IsolationFactory::new(cfg.clone());
    let lifecycles = factory
        .get_isolation_services(&TEST_TID, true)
        .await
        .unwrap_or_else(|e| panic!("Failed to create lifecycle: {}", e));
    let gpu_resource = GpuResourceTracker::boxed(
        &cfg.container_resources.gpu_resource,
        &cfg.container_resources,
        &TEST_TID,
        &lifecycles.get(&Isolation::DOCKER),
        &cfg.status,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to create gpu resource man: {}", e));

    let cm = ContainerManager::boxed(
        cfg.container_resources.clone(),
        lifecycles.clone(),
        gpu_resource.clone(),
        &TEST_TID,
    )
    .await
    .unwrap_or_else(|e| panic!("Failed to create container manger for test: {}", e));
    let reg = RegistrationService::new(
        cm.clone(),
        lifecycles.clone(),
        cfg.limits.clone(),
        cmap.clone(),
        cfg.container_resources.clone(),
    );
    #[cfg(feature = "power_cap")]
    let en_log = EnergyLogger::boxed(None, &TEST_TID)
        .await
        .unwrap_or_else(|e| panic!("Failed to create energy logger: {}", e));
    #[cfg(feature = "power_cap")]
    let energy =
        EnergyLimiter::boxed(&None, en_log).unwrap_or_else(|e| panic!("Failed to create energy limiter: {}", e));
    let invoker_fact = InvokerFactory::new(
        cm.clone(),
        cfg.limits.clone(),
        cfg.invocation.clone(),
        cmap,
        cpu,
        gpu_resource,
        cfg.container_resources.gpu_resource.clone(),
        &reg,
        #[cfg(feature = "power_cap")]
        energy,
    );
    let invoker = invoker_fact
        .get_invoker_service(&TEST_TID)
        .unwrap_or_else(|e| panic!("Failed to create invoker service because: {}", e));
    (_log, cfg, cm, invoker, reg)
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
        language: LanguageRuntime::Nolang.into(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::CONTAINERD.bits(),
        resource_timings_json: "".to_string(),
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
        language: LanguageRuntime::Nolang.into(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::CONTAINERD.bits(),
        resource_timings_json: "".to_string(),
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
            if invok_svc.queue_len().get(&compute_queue).unwrap().len >= len {
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
