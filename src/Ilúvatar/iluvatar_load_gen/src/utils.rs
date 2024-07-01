use crate::benchmark::BenchmarkStore;
use anyhow::{Context, Result};
use iluvatar_controller_library::services::ControllerAPI;
use iluvatar_library::{
    logging::LocalTime,
    transaction::{gen_tid, TransactionId},
    types::{CommunicationMethod, Compute, Isolation, MemSizeMb, ResourceTimings},
    utils::{port::Port, timing::TimedExt},
};
use iluvatar_rpc::rpc::{CleanResponse, ContainerState, InvokeRequest, InvokeResponse, RegisterRequest};
use iluvatar_rpc::rpc::{LanguageRuntime, PrewarmRequest};
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{runtime::Runtime, task::JoinHandle};

lazy_static::lazy_static! {
  pub static ref VERSION: String = "0.0.1".to_string();
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Target {
    Worker,
    Controller,
}
#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum LoadType {
    Lookbusy,
    Functions,
}
#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RunType {
    Live,
    Simulation,
}

#[derive(Serialize, Deserialize)]
pub struct ThreadResult {
    pub thread_id: usize,
    pub data: Vec<CompletedWorkerInvocation>,
    pub registration: RegistrationResult,
    pub errors: u64,
}
impl Ord for ThreadResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.thread_id.cmp(&other.thread_id)
    }
}
impl PartialOrd for ThreadResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for ThreadResult {}
impl PartialEq for ThreadResult {
    fn eq(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id
    }
}

#[derive(Serialize, Deserialize)]
pub struct RegistrationResult {
    pub duration_us: u128,
    pub result: String,
}
#[derive(Serialize, Deserialize)]
/// This is the output from the python functions
pub struct FunctionExecOutput {
    pub body: Body,
}
#[derive(Serialize, Deserialize)]
pub struct Body {
    pub cold: bool,
    pub start: f64,
    pub end: f64,
    /// python runtime latency in seconds
    pub latency: f64,
}
#[derive(Serialize, Deserialize)]
pub struct CompletedWorkerInvocation {
    /// The RPC result returned by the worker
    pub worker_response: InvokeResponse,
    /// The deserialized result of the function's execution
    pub function_output: FunctionExecOutput,
    /// The latency experienced by the client, in microseconds
    pub client_latency_us: u128,
    pub function_name: String,
    pub function_version: String,
    pub tid: TransactionId,
    pub invoke_start: String,
}
impl CompletedWorkerInvocation {
    pub fn error(
        msg: String,
        name: &str,
        version: &str,
        tid: &TransactionId,
        invoke_start: String,
        invok_lat: Duration,
    ) -> Self {
        CompletedWorkerInvocation {
            worker_response: InvokeResponse::error(&msg),
            function_output: FunctionExecOutput {
                body: Body {
                    cold: false,
                    start: 0.0,
                    end: 0.0,
                    latency: 0.0,
                },
            },
            client_latency_us: invok_lat.as_micros(),
            function_name: name.to_owned(),
            function_version: version.to_owned(),
            tid: tid.clone(),
            invoke_start,
        }
    }
}
impl Ord for CompletedWorkerInvocation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.invoke_start.cmp(&other.invoke_start)
    }
}
impl PartialOrd for CompletedWorkerInvocation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for CompletedWorkerInvocation {}
impl PartialEq for CompletedWorkerInvocation {
    fn eq(&self, other: &Self) -> bool {
        self.invoke_start == other.invoke_start
    }
}

#[derive(Serialize, Deserialize)]
pub struct CompletedControllerInvocation {
    /// The RPC result returned by the worker
    pub controller_response: InvokeResponse,
    /// The deserialized result of the function's execution
    pub function_output: FunctionExecOutput,
    /// The latency experienced by the client, in microseconds
    pub client_latency_us: u128,
    pub function_name: String,
    pub function_version: String,
    pub invoke_start: String,
    pub transaction_id: TransactionId,
}
impl CompletedControllerInvocation {
    pub fn error(msg: String, name: &str, version: &str, tid: &TransactionId, invoke_start: String) -> Self {
        CompletedControllerInvocation {
            controller_response: InvokeResponse {
                json_result: msg,
                success: false,
                duration_us: 0,
                compute: Compute::empty().bits(),
                container_state: ContainerState::Error.into(),
            },
            function_output: FunctionExecOutput {
                body: Body {
                    cold: false,
                    start: 0.0,
                    end: 0.0,
                    latency: 0.0,
                },
            },
            client_latency_us: 0,
            function_name: name.to_owned(),
            function_version: version.to_owned(),
            invoke_start,
            transaction_id: tid.clone(),
        }
    }
}
impl Ord for CompletedControllerInvocation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.invoke_start.cmp(&other.invoke_start)
    }
}
impl PartialOrd for CompletedControllerInvocation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for CompletedControllerInvocation {}
impl PartialEq for CompletedControllerInvocation {
    fn eq(&self, other: &Self) -> bool {
        self.invoke_start == other.invoke_start
    }
}

/// Load benchmark data if the path contains a vaild path.
/// Raises an error if the file is missing or parsing fails
pub fn load_benchmark_data(path: &Option<String>) -> Result<Option<BenchmarkStore>> {
    match path {
        Some(pth) => {
            // Choosing functions from json file benchmark data
            let contents = std::fs::read_to_string(pth).context("Something went wrong reading the benchmark file")?;
            match serde_json::from_str::<BenchmarkStore>(&contents) {
                Ok(d) => Ok(Some(d)),
                Err(e) => anyhow::bail!("Failed to read and parse benchmark data! '{}'", e),
            }
        }
        None => Ok(None),
    }
}

/// Run an invocation against the controller
/// Return the [ControllerInvokeResult] result after parsing
/// also return the latency in milliseconds of the request
pub async fn controller_invoke(
    name: &str,
    version: &str,
    json_args: Option<String>,
    clock: Arc<LocalTime>,
    api: ControllerAPI,
) -> Result<CompletedControllerInvocation> {
    let tid = gen_tid();
    let req = InvokeRequest {
        function_name: name.to_owned(),
        function_version: version.to_owned(),
        json_args: match json_args {
            Some(json_args) => json_args,
            None => "{}".to_owned(),
        },
        transaction_id: tid.clone(),
    };
    let invoke_start = clock.now_str()?;
    let (r, invoke_lat) = api.invoke(req).timed().await;
    let r = match r {
        Ok(response) => match serde_json::from_str::<FunctionExecOutput>(&response.json_result) {
            Ok(feo) => CompletedControllerInvocation {
                controller_response: response,
                function_output: feo,
                client_latency_us: invoke_lat.as_micros(),
                function_name: name.to_owned(),
                function_version: version.to_owned(),
                invoke_start,
                transaction_id: tid,
            },
            Err(e) => CompletedControllerInvocation::error(
                format!(
                    "FunctionExecOutput Deserialization error: {}; {}",
                    e, &response.json_result
                ),
                name,
                version,
                &tid,
                invoke_start,
            ),
        },
        Err(e) => {
            CompletedControllerInvocation::error(format!("Invocation error: {}", e), name, version, &tid, invoke_start)
        }
    };
    Ok(r)
}

pub async fn controller_register(
    name: &str,
    version: &str,
    image: &str,
    memory: MemSizeMb,
    timings: Option<&ResourceTimings>,
    api: ControllerAPI,
) -> Result<Duration> {
    let start = Instant::now();
    let tid = format!("{}-{}-reg", name, version);
    let req = RegisterRequest::new(
        name,
        version,
        image,
        1,
        memory,
        timings,
        LanguageRuntime::Python3,
        Compute::CPU,
        Isolation::CONTAINERD,
        &tid,
    )?;
    match api.register(req).await {
        Ok(_) => Ok(start.elapsed()),
        Err(e) => Err(e),
    }
}

pub async fn controller_prewarm(
    name: &str,
    version: &str,
    api: ControllerAPI,
    tid: &TransactionId,
) -> Result<Duration> {
    let start = Instant::now();
    let req = PrewarmRequest {
        function_name: name.to_owned(),
        function_version: version.to_owned(),
        transaction_id: tid.to_owned(),
        compute: Compute::CPU.bits(),
    };
    match api.prewarm(req).await {
        Ok(_) => Ok(start.elapsed()),
        Err(e) => Err(e),
    }
}

pub async fn worker_register(
    name: String,
    version: &str,
    image: String,
    memory: MemSizeMb,
    host: String,
    port: Port,
    factory: &Arc<WorkerAPIFactory>,
    comm_method: Option<CommunicationMethod>,
    isolation: Isolation,
    compute: Compute,
    timings: Option<&ResourceTimings>,
) -> Result<(String, Duration, TransactionId)> {
    let tid: TransactionId = format!("{}-reg-tid", name);
    let method = match comm_method {
        Some(m) => m,
        None => CommunicationMethod::RPC,
    };
    let mut api = factory.get_worker_api(&host, &host, port, method, &tid).await?;
    let (reg_out, reg_dur) = api
        .register(
            name,
            version.to_owned(),
            image,
            memory,
            1,
            1,
            tid.clone(),
            isolation,
            compute,
            timings,
        )
        .timed()
        .await;

    match reg_out {
        Ok(s) => match serde_json::from_str::<HashMap<String, String>>(&s) {
            Ok(r) => match r.get("Ok") {
                Some(_) => Ok((s, reg_dur, tid)),
                None => anyhow::bail!("worker registration did not have 'Ok', got {:?}", r),
            },
            Err(e) => anyhow::bail!("worker registration parsing '{:?}' failed because {:?}", s, e),
        },
        Err(e) => anyhow::bail!("worker registration encoutered an error because {:?}", e),
    }
}

pub async fn worker_prewarm(
    name: &str,
    version: &str,
    host: &str,
    port: Port,
    tid: &TransactionId,
    factory: &Arc<WorkerAPIFactory>,
    comm_method: Option<CommunicationMethod>,
    compute: Compute,
) -> Result<(String, Duration)> {
    let method = match comm_method {
        Some(m) => m,
        None => CommunicationMethod::RPC,
    };
    let mut api = factory.get_worker_api(host, host, port, method, tid).await?;
    let (res, dur) = api
        .prewarm(name.to_owned(), version.to_owned(), tid.to_string(), compute)
        .timed()
        .await;
    match res {
        Ok(s) => Ok((s, dur)),
        Err(e) => anyhow::bail!("worker prewarm failed because {:?}", e),
    }
}

pub async fn worker_invoke(
    name: &str,
    version: &str,
    host: &str,
    port: Port,
    tid: &TransactionId,
    args: Option<String>,
    clock: Arc<LocalTime>,
    factory: &Arc<WorkerAPIFactory>,
    comm_method: Option<CommunicationMethod>,
) -> Result<CompletedWorkerInvocation> {
    let args = match args {
        Some(a) => a,
        None => "{}".to_string(),
    };
    let method = match comm_method {
        Some(m) => m,
        None => CommunicationMethod::RPC,
    };
    let invoke_start = clock.now_str()?;
    let mut api = match factory.get_worker_api(host, host, port, method, tid).await {
        Ok(a) => a,
        Err(e) => anyhow::bail!("API creation error: {:?}", e),
    };

    let (invok_out, invok_lat) = api
        .invoke(name.to_owned(), version.to_owned(), args, tid.to_owned())
        .timed()
        .await;
    let c = match invok_out {
        Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.json_result) {
            Ok(b) => CompletedWorkerInvocation {
                worker_response: r,
                function_output: b,
                client_latency_us: invok_lat.as_micros(),
                function_name: name.to_owned(),
                function_version: version.to_owned(),
                tid: tid.to_owned(),
                invoke_start,
            },
            Err(e) => CompletedWorkerInvocation::error(
                format!("Deserialization error: {}; {}", e, r.json_result),
                name,
                version,
                tid,
                invoke_start,
                invok_lat,
            ),
        },
        Err(e) => CompletedWorkerInvocation::error(
            format!("Invocation error: {:?}", e),
            name,
            version,
            tid,
            invoke_start,
            invok_lat,
        ),
    };
    Ok(c)
}

pub async fn worker_clean(
    host: &str,
    port: Port,
    tid: &TransactionId,
    factory: &Arc<WorkerAPIFactory>,
    comm_method: Option<CommunicationMethod>,
) -> Result<CleanResponse> {
    let method = match comm_method {
        Some(m) => m,
        None => CommunicationMethod::RPC,
    };
    let mut api = match factory.get_worker_api(host, host, port, method, tid).await {
        Ok(a) => a,
        Err(e) => anyhow::bail!("API creation error: {:?}", e),
    };
    api.clean(tid.clone()).await
}

/// How to handle per-thread errors that appear when joining load workers
pub enum ErrorHandling {
    Raise,
    Print,
    Ignore,
}

/// Resolve all the tokio threads and return their results
/// Optionally handle errors from threads
pub fn resolve_handles<T>(
    runtime: &Runtime,
    run_results: Vec<JoinHandle<Result<T>>>,
    eh: ErrorHandling,
) -> Result<Vec<T>>
where
    T: Ord,
{
    let mut ret = vec![];
    for h in run_results {
        match runtime.block_on(h) {
            Ok(r) => match r {
                Ok(ok) => ret.push(ok),
                Err(e) => match eh {
                    ErrorHandling::Raise => return Err(e),
                    ErrorHandling::Print => println!("Error from thread: {:?}", e),
                    ErrorHandling::Ignore => (),
                },
            },
            Err(thread_e) => println!("Joining error: {}", thread_e),
        };
    }
    ret.sort();
    Ok(ret)
}

/// Save worker load results as a csv
pub fn save_worker_result_csv<P: AsRef<Path> + std::fmt::Debug>(
    path: P,
    run_results: &Vec<CompletedWorkerInvocation>,
) -> Result<()> {
    let mut f = match File::create(&path) {
        Ok(f) => f,
        Err(e) => {
            anyhow::bail!("Failed to create csv output '{:?}' file because {}", &path, e);
        }
    };
    let to_write =
        "success,function_name,was_cold,worker_duration_us,code_duration_sec,e2e_duration_us,tid\n".to_string();
    match f.write_all(to_write.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
            anyhow::bail!("Failed to write json header to '{:?}' of result because {}", &path, e);
        }
    };

    for worker_invocation in run_results {
        let to_write = format!(
            "{},{},{},{},{},{},{}\n",
            worker_invocation.worker_response.success,
            worker_invocation.function_name,
            worker_invocation.function_output.body.cold,
            worker_invocation.worker_response.duration_us,
            worker_invocation.function_output.body.latency,
            worker_invocation.client_latency_us,
            worker_invocation.tid
        );
        match f.write_all(to_write.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                println!("Failed to write result to '{:?}' because {}", &path, e);
                continue;
            }
        };
    }
    Ok(())
}

pub fn save_result_json<P: AsRef<Path> + std::fmt::Debug, T: Serialize>(path: P, results: &T) -> Result<()> {
    let mut f = match File::create(&path) {
        Ok(f) => f,
        Err(e) => {
            anyhow::bail!("Failed to create json output '{:?}' file because {}", &path, e);
        }
    };

    let to_write = match serde_json::to_string(&results) {
        Ok(f) => f,
        Err(e) => {
            anyhow::bail!("Failed to convert results to json because {}", e);
        }
    };
    f.write_all(to_write.as_bytes())?;
    Ok(())
}
