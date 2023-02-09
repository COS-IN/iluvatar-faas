use std::{sync::Arc, time::SystemTime};
use dashmap::DashSet;
use iluvatar_library::{transaction::TransactionId, types::{MemSizeMb, Isolation}, utils::{execute_cmd, port::free_local_port}, bail_error};
use crate::{worker_api::worker_config::{ContainerResources, FunctionLimits}, services::containers::structs::ContainerState};
use self::dockerstructs::DockerContainer;
use super::{structs::{RegisteredFunction, Container}, LifecycleService};
use anyhow::Result;
use guid_create::GUID;
use tracing::{warn, info, trace, debug};

pub mod dockerstructs;

#[derive(Debug)]
#[allow(unused)]
pub struct DockerLifecycle {
  config: Arc<ContainerResources>,
  limits_config: Arc<FunctionLimits>,
  creation_sem: Option<tokio::sync::Semaphore>,
  pulled_images: DashSet<String>
}

impl DockerLifecycle {
  pub fn supported(tid: &TransactionId) -> bool {
    let args = vec!["ps"];
    match execute_cmd("/usr/bin/docker", &args, None, tid) {
      Ok(out) => out.status.success(),
      Err(_) => false,
    }
  }

  pub fn new(config: Arc<ContainerResources>, limits_config: Arc<FunctionLimits>) -> Self {
    let sem = match config.concurrent_creation {
      0 => None,
      i => Some(tokio::sync::Semaphore::new(i as usize))
    };
    DockerLifecycle {
      config,
      limits_config,
      creation_sem: sem,
      pulled_images: DashSet::new()
    }
  }

  /// Get the stdout and stderr of a container
  fn get_logs(&self, container: &Container, tid: &TransactionId) -> Result<(String, String)> {
    let args = vec!["logs", container.container_id().as_str()];
    let output = execute_cmd("/usr/bin/docker", &args, None, tid)?;
    if let Some(status) = output.status.code() {
      if status != 0 {
        bail_error!(tid=%tid, status=status, output=?output, "Failed to get docker logs with exit code");
      }
    } else {
      bail_error!(tid=%tid, output=?output, "Failed to get docker logs no exit code");
    }
    Ok( (String::from_utf8_lossy(&output.stdout).to_string(), String::from_utf8_lossy(&output.stderr).to_string()) )
  }

  fn get_stderr(&self, container: &Container, tid: &TransactionId) -> Result<String> {
    let (_out, err) = self.get_logs(container, tid)?;
    Ok(err)
  }

  fn get_stdout(&self, container: &Container, tid: &TransactionId) -> Result<String> {
    let (out, _err) = self.get_logs(container, tid)?;
    Ok(out)
  }
}

#[tonic::async_trait]
#[allow(unused)]
impl LifecycleService for DockerLifecycle {
  fn backend(&self) -> Isolation {
    Isolation::DOCKER
  }

  /// creates and starts the entrypoint for a container based on the given image
  /// Run inside the specified namespace
  /// returns a new, unique ID representing it
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, fqdn, image_name, parallel_invokes, namespace, mem_limit_mb, cpus), fields(tid=%tid)))]
  async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    let cid = format!("{}-{}", fqdn, GUID::rand());
    let port = free_local_port()?;
    let gunicorn_args = format!("GUNICORN_CMD_ARGS=--bind 0.0.0.0:{}", port);
    let bind_args = format!("--bind 0.0.0.0:{}", port);
    let port_args = format!("{}:{}", port, port);
    let il_port = format!("__IL_PORT={}", port);

    let args = vec!["run", "--detach", "--name", cid.as_str(), "-e", gunicorn_args.as_str(), "-e", il_port.as_str(), "-e", "__IL_HOST=0.0.0.0", "--label", "owner=iluvatar_worker", "--cpus", "1", "-p", port_args.as_str(), image_name.as_str(), "-w 1"];

    let permit = match &self.creation_sem {
      Some(sem) => match sem.acquire().await {
        Ok(p) => {
          debug!(tid=%tid, "Acquired docker creation semaphore");
          Some(p)
        },
        Err(e) => {
          bail_error!(error=%e, tid=%tid, "Error trying to acquire docker creation semaphore");
        },
      },
      None => None,
    };

    let output = execute_cmd("/usr/bin/docker", &args, None, tid)?;
    if let Some(status) = output.status.code() {
      if status != 0 {
        bail_error!(tid=%tid, status=status, output=?output, "Failed to create docker container with exit code");
      }
    } else {
      bail_error!(tid=%tid, output=?output, "Failed to create docker container with no exit code");
    }
    drop(permit);
    debug!(tid=%tid, "Dropped docker creation semaphore after load_mounts error");
    debug!(tid=%tid, name=%image_name, containerid=%cid, output=?output, "Docker container started successfully");
    info!(tid=%tid, name=%image_name, containerid=%cid, "Docker container started successfully");
    unsafe {
      let c = DockerContainer::new(cid, port, "0.0.0.0".to_string(), std::num::NonZeroU32::new_unchecked(parallel_invokes), &fqdn, &reg, self.limits_config.timeout_sec, ContainerState::Cold)?;
      Ok(Arc::new(c))
    }
  }

  /// Removed the specified container in the containerd namespace
  async fn remove_container(&self, container: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    let output = execute_cmd("/usr/bin/docker", &vec!["rm", "--force", container.container_id().as_str()], None, tid)?;
    if let Some(status) = output.status.code() {
      if status != 0 {
        bail_error!(tid=%tid, container_id=%container.container_id(), status=status, output=?output, "Failed to remove docker container with exit code");
      }
    } else {
      bail_error!(tid=%tid, container_id=%container.container_id(), output=?output, "Failed to remove docker container with no exit code");
    }
    Ok(())
  }

  async fn prepare_function_registration(&self, function_name: &String, function_version: &String, image_name: &String, memory: MemSizeMb, cpus: u32, parallel_invokes: u32, _fqdn: &String, tid: &TransactionId) -> Result<RegisteredFunction> {
    let ret = RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory,
      cpus,
      snapshot_base: "".to_string(),
      parallel_invokes,
      isolation_type: self.backend()
    };
    if self.pulled_images.contains(image_name) {
      return Ok(ret);
    }

    let output = execute_cmd("/usr/bin/docker", &vec!["pull", image_name.as_str()], None, tid)?;
    if let Some(status) = output.status.code() {
      if status != 0 {
        bail_error!(tid=%tid, status=status, output=?output, "Failed to pull docker image with exit code");
      }
    } else {
      bail_error!(tid=%tid, output=?output, "Failed to pull docker image with no exit code");
    }
    trace!(tid=%tid, name=%image_name, output=?output, "Docker image pulled successfully");
    info!(tid=%tid, name=%image_name, "Docker image pulled successfully");
    self.pulled_images.insert(image_name.clone());
    Ok(ret)
  }
  
  async fn clean_containers(&self, ctd_namespace: &str, self_src: Arc<dyn LifecycleService>, tid: &TransactionId) -> Result<()> {
    let output = execute_cmd("/usr/bin/docker", &vec!["ps", "--filter", "label=owner=iluvatar_worker", "-q"], None, tid)?;
    if let Some(status) = output.status.code() {
      if status != 0 {
        bail_error!(tid=%tid, status=status, output=?output, "Failed to run 'docker ps' with exit code");
      }
    } else {
      bail_error!(tid=%tid, output=?output, "Failed to run 'docker ps' with no exit code");
    }
    let cow = String::from_utf8_lossy(&output.stdout);
    let stdout: Vec<&str> = cow.split("\n").filter(|str| str.len() > 0).collect();
    for docker_id in stdout {
      let output = execute_cmd("/usr/bin/docker", &vec!["rm", "--force", docker_id], None, tid)?;
      if let Some(status) = output.status.code() {
        if status != 0 {
          bail_error!(tid=%tid, docker_id=%docker_id, status=status, output=?output, "Failed to remove docker container with exit code");
        }
      } else {
        bail_error!(tid=%tid, docker_id=%docker_id, output=?output, "Failed to remove docker container with no exit code");
      }
    }

    Ok(())
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container, timeout_ms), fields(tid=%tid)))]
  async fn wait_startup(&self, container: &Container, timeout_ms: u64, tid: &TransactionId) -> Result<()> {
    let start = SystemTime::now();
    loop {
      match self.get_logs(container, tid) {
        Ok( (out, err) ) => {
          // stderr was written to, gunicorn server is either up or crashed
          if err.len() > 0 {
            break;
          }
        },
        Err(e) => {
          bail_error!(tid=%tid, container_id=%container.container_id(), error=%e, "Timeout while reading inotify events for docker container");
        },
        _ => bail_error!(tid=%tid, container_id=%container.container_id(), "Error while reading inotify events for docker container"),
      };
      if start.elapsed()?.as_millis() as u64 >= timeout_ms {
        let stdout = self.read_stdout(&container, tid);
        let stderr = self.read_stderr(&container, tid);
        if stderr.len() > 0 {
          warn!(tid=%tid, container_id=%&container.container_id(), "Timeout waiting for docker container start, but stderr was written to?");
          return Ok(())
        }
        bail_error!(tid=%tid, container_id=%container.container_id(), stdout=%stdout, stderr=%stderr, "Timeout while monitoring logs for docker container");
      }
      tokio::time::sleep(std::time::Duration::from_micros(100)).await;
    }
   Ok(())
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
  fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
    let cast_container = match crate::services::containers::structs::cast::<DockerContainer>(&container, tid) {
      Ok(c) => c,
      Err(e) => { 
        warn!(tid=%tid, error=%e, "Error casting container to DockerContainer");
        return container.get_curr_mem_usage();
      },
    };
    cast_container.function.memory
  }

  fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
    match self.get_stdout(container, tid) {
      Ok(out) => out,
      Err(_) => "".to_string(),
    }
  }
  fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
    match self.get_stderr(container, tid) {
      Ok(err) => err,
      Err(_) => "".to_string(),
    }
  }
}
impl crate::services::containers::structs::ToAny for DockerLifecycle {
  fn as_any(&self) -> &dyn std::any::Any {
      self
  }
}
