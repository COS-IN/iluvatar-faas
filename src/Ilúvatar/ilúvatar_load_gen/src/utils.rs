use anyhow::{Context, Result};
use iluvatar_controller_library::controller::controller_structs::json::{
    ControllerInvokeResult, Invoke, Prewarm, RegisterFunction,
};
use iluvatar_library::{
    logging::LocalTime,
    transaction::TransactionId,
    types::{CommunicationMethod, Compute, Isolation, MemSizeMb, ResourceTimings},
    utils::{port::Port, timing::TimedExt},
};
use iluvatar_worker_library::{
    rpc::{CleanResponse, ContainerState, InvokeResponse},
    worker_api::worker_comm::WorkerAPIFactory,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Write, path::Path, sync::Arc, time::Duration};
use tokio::{runtime::Runtime, task::JoinHandle};

use crate::benchmark::BenchmarkStore;

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
        self.thread_id.partial_cmp(&other.thread_id)
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
    pub fn error(msg: String, name: &String, version: &String, tid: &TransactionId, invoke_start: String) -> Self {
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
            client_latency_us: 0,
            function_name: name.clone(),
            function_version: version.clone(),
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
        self.invoke_start.partial_cmp(&other.invoke_start)
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
    pub controller_response: ControllerInvokeResult,
    /// The deserialized result of the function's execution
    pub function_output: FunctionExecOutput,
    /// The latency experienced by the client, in microseconds
    pub client_latency_us: u128,
    pub function_name: String,
    pub function_version: String,
    pub invoke_start: String,
}
impl CompletedControllerInvocation {
    pub fn error(
        msg: String,
        name: &String,
        version: &String,
        tid: Option<&TransactionId>,
        invoke_start: String,
    ) -> Self {
        let r_tid = match tid {
            Some(t) => t.clone(),
            None => "ERROR_TID".to_string(),
        };
        CompletedControllerInvocation {
            controller_response: ControllerInvokeResult {
                worker_duration_us: 0,
                success: false,
                tid: r_tid,
                result: InvokeResponse {
                    json_result: msg,
                    success: false,
                    duration_us: 0,
                    compute: Compute::empty().bits(),
                    container_state: ContainerState::Error.into(),
                },
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
            function_name: name.clone(),
            function_version: version.clone(),
            invoke_start,
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
        self.invoke_start.partial_cmp(&other.invoke_start)
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
    name: &String,
    version: &String,
    host: &String,
    port: Port,
    args: Option<Vec<String>>,
    clock: Arc<LocalTime>,
    client: Arc<Client>,
) -> Result<CompletedControllerInvocation> {
    let req = Invoke {
        function_name: name.clone(),
        function_version: version.clone(),
        args,
    };
    let invoke_start = clock.now_str()?;
    let (invok_out, invok_lat) = client
        .post(format!("http://{}:{}/invoke", &host, port))
        .json(&req)
        .header("Content-Type", "application/json")
        .send()
        .timed()
        .await;
    let r = match invok_out {
        Ok(r) => {
            let txt = match r.text().await {
                Ok(t) => t,
                Err(e) => {
                    return Ok(CompletedControllerInvocation::error(
                        format!("Get text error: {};", e),
                        &name,
                        &version,
                        None,
                        invoke_start,
                    ))
                }
            };
            match serde_json::from_str::<ControllerInvokeResult>(&txt) {
                Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.result.json_result) {
                    Ok(feo) => CompletedControllerInvocation {
                        controller_response: r,
                        function_output: feo,
                        client_latency_us: invok_lat.as_micros(),
                        function_name: name.clone(),
                        function_version: version.clone(),
                        invoke_start,
                    },
                    Err(e) => CompletedControllerInvocation::error(
                        format!("FunctionExecOutput Deserialization error: {}; {}", e, &txt),
                        &name,
                        &version,
                        Some(&r.tid),
                        invoke_start,
                    ),
                },
                Err(e) => CompletedControllerInvocation::error(
                    format!("ControllerInvokeResult Deserialization error: {}; {}", e, &txt),
                    &name,
                    &version,
                    None,
                    invoke_start,
                ),
            }
        }
        Err(e) => CompletedControllerInvocation::error(
            format!("Invocation error: {}", e),
            &name,
            &version,
            None,
            invoke_start,
        ),
    };
    Ok(r)
}

pub async fn controller_register(
    name: &String,
    version: &String,
    image: &String,
    memory: MemSizeMb,
    host: &String,
    port: Port,
    timings: Option<&ResourceTimings>,
) -> Result<Duration> {
    let req = RegisterFunction {
        function_name: name.clone(),
        function_version: version.clone(),
        image_name: image.clone(),
        memory,
        cpus: 1,
        parallel_invokes: 1,
        timings: match timings {
            Some(r) => Some(r.clone()),
            None => None,
        },
    };
    let client = reqwest::Client::new();
    let (reg_out, reg_dur) = client
        .post(format!("http://{}:{}/register_function", &host, port))
        .json(&req)
        .header("Content-Type", "application/json")
        .send()
        .timed()
        .await;
    match reg_out {
        Ok(r) => {
            let status = r.status();
            if status == reqwest::StatusCode::OK {
                Ok(reg_dur)
            } else {
                let text = r.text().await?;
                anyhow::bail!(
                    "Got unexpected HTTP status when registering function with the controller '{}'; text: {}",
                    status,
                    text
                );
            }
        }
        Err(e) => {
            anyhow::bail!(
                "HTTP error when trying to register function with the controller '{}'",
                e
            );
        }
    }
}

pub async fn controller_prewarm(name: &String, version: &String, host: &String, port: Port) -> Result<Duration> {
    let req = Prewarm {
        function_name: name.clone(),
        function_version: version.clone(),
    };
    let client = reqwest::Client::new();
    let (reg_out, reg_dur) = client
        .post(format!("http://{}:{}/prewarm", &host, port))
        .json(&req)
        .header("Content-Type", "application/json")
        .send()
        .timed()
        .await;
    match reg_out {
        Ok(r) => {
            let status = r.status();
            if status == reqwest::StatusCode::ACCEPTED {
                Ok(reg_dur)
            } else {
                let text = r.text().await?;
                anyhow::bail!(
                    "Got unexpected HTTP status when prewarming function with the controller '{}'; text: {}",
                    status,
                    text
                );
            }
        }
        Err(e) => anyhow::bail!(
            "HTTP error when trying to prewarming function with the controller '{}'",
            e
        ),
    }
}

pub async fn worker_register(
    name: String,
    version: &String,
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
            version.clone(),
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
    name: &String,
    version: &String,
    host: &String,
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
    let mut api = factory.get_worker_api(&host, &host, port, method, &tid).await?;
    let (res, dur) = api
        .prewarm(name.clone(), version.clone(), tid.to_string(), compute)
        .timed()
        .await;
    match res {
        Ok(s) => Ok((s, dur)),
        Err(e) => anyhow::bail!("worker prewarm failed because {:?}", e),
    }
}

pub async fn worker_invoke(
    name: &String,
    version: &String,
    host: &String,
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
    let mut api = match factory.get_worker_api(&host, &host, port, method, &tid).await {
        Ok(a) => a,
        Err(e) => anyhow::bail!("API creation error: {:?}", e),
    };

    let (invok_out, invok_lat) = api
        .invoke(name.clone(), version.clone(), args, tid.clone())
        .timed()
        .await;
    let c = match invok_out {
        Ok(r) => match serde_json::from_str::<FunctionExecOutput>(&r.json_result) {
            Ok(b) => CompletedWorkerInvocation {
                worker_response: r,
                function_output: b,
                client_latency_us: invok_lat.as_micros(),
                function_name: name.clone(),
                function_version: version.clone(),
                tid: tid.clone(),
                invoke_start: invoke_start,
            },
            Err(e) => CompletedWorkerInvocation::error(
                format!("Deserialization error: {}; {}", e, r.json_result),
                &name,
                &version,
                &tid,
                invoke_start,
            ),
        },
        Err(e) => CompletedWorkerInvocation::error(
            format!("Invocation error: {:?}", e),
            &name,
            &version,
            &tid,
            invoke_start,
        ),
    };
    Ok(c)
}

pub async fn worker_clean(
    host: &String,
    port: Port,
    tid: &TransactionId,
    factory: &Arc<WorkerAPIFactory>,
    comm_method: Option<CommunicationMethod>,
) -> Result<CleanResponse> {
    let method = match comm_method {
        Some(m) => m,
        None => CommunicationMethod::RPC,
    };
    let mut api = match factory.get_worker_api(&host, &host, port, method, &tid).await {
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
    let to_write = format!("success,function_name,was_cold,worker_duration_us,code_duration_sec,e2e_duration_us,tid\n");
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
