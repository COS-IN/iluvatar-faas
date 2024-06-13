use self::dockerstructs::DockerContainer;
use super::{structs::Container, ContainerIsolationService};
use crate::{
    services::{containers::structs::ContainerState, registration::RegisteredFunction},
    worker_api::worker_config::{ContainerResourceConfig, FunctionLimits},
};
use bollard::Docker;
use bollard::{image::CreateImageOptions, container::{ListContainersOptions,RemoveContainerOptions,StatsOptions}};
use futures::StreamExt;
use anyhow::Result;
use dashmap::DashSet;
use guid_create::GUID;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
    utils::{execute_cmd_async, port::free_local_port},
};
use std::collections::HashMap;
use std::{sync::Arc, time::SystemTime};
use tracing::{debug, error, info, warn};
pub mod dockerstructs;

const OWNER_TAG: &str = "owner=iluvatar_worker";

#[derive(Debug)]
#[allow(unused)]
pub struct DockerIsolation {
    config: Arc<ContainerResourceConfig>,
    limits_config: Arc<FunctionLimits>,
    creation_sem: Option<tokio::sync::Semaphore>,
    pulled_images: DashSet<String>,
    docker_api: Docker,
}

impl DockerIsolation {
    pub async fn supported(tid: &TransactionId) -> bool {
        let docker = match Docker::connect_with_socket_defaults() {
            Ok(d) => d,
            Err(e) => {
              warn!(tid=%tid, error=%e, "Failed to connect to docker");
              return false;
            },
        };
        match docker.version().await {
            Ok(_) => true,
            Err(e) => {
                warn!(tid=%tid, error=%e, "Failed to query docker version");
                false
            },
        }
    }

    pub fn new(
        config: Arc<ContainerResourceConfig>,
        limits_config: Arc<FunctionLimits>,
        tid: &TransactionId,
    ) -> Result<Self> {
        let docker = match Docker::connect_with_socket_defaults() {
            Ok(d) => d,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to connect to docker"),
        };
        let sem = match config.concurrent_creation {
            0 => None,
            i => Some(tokio::sync::Semaphore::new(i as usize)),
        };
        Ok(DockerIsolation {
            config,
            limits_config,
            creation_sem: sem,
            pulled_images: DashSet::new(),
            docker_api: docker,
        })
    }

    pub async fn docker_run<'a>(
        &self,
        mut run_args: Vec<&'a str>,
        image_name: &'a str,
        container_id: &str,
        proc_args: Option<Vec<&'a str>>,
        tid: &TransactionId,
        env: Option<&HashMap<String, String>>,
    ) -> Result<()> {
        run_args.insert(0, "run");
        run_args.extend(["--label", OWNER_TAG, "--detach", image_name]);
        if let Some(a) = proc_args {
            run_args.extend(a);
        }
        let output = execute_cmd_async("/usr/bin/docker", run_args, env, tid).await?;
        match output.status.code() {
            Some(0) => {
                debug!(tid=%tid, name=%image_name, containerid=%container_id, output=?output, "Docker container started successfully");
                info!(tid=%tid, name=%image_name, containerid=%container_id, "Docker container started successfully");
                Ok(())
            }
            Some(error_stat) => {
                bail_error!(tid=%tid, status=error_stat, output=?output, "Failed to create docker container with exit code")
            }
            None => bail_error!(tid=%tid, output=?output, "Failed to create docker container with no exit code"),
        }
    }

    /// Get the stdout and stderr of a container
    pub async fn get_logs(&self, container_id: &str, tid: &TransactionId) -> Result<(String, String)> {
        let args = vec!["logs", container_id];
        let output = execute_cmd_async("/usr/bin/docker", args, None, tid).await?;
        match output.status.code() {
            Some(0) => Ok((
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
            )),
            Some(error_stat) => {
                bail_error!(tid=%tid, container_id=%container_id, status=error_stat, output=?output, "Failed to get docker logs with exit code")
            }
            None => {
                bail_error!(tid=%tid, container_id=%container_id, output=?output, "Failed to get docker logs no exit code")
            }
        }
    }

    async fn get_stderr(&self, container: &Container, tid: &TransactionId) -> Result<String> {
        let (_out, err) = self.get_logs(container.container_id(), tid).await?;
        Ok(err)
    }

    async fn get_stdout(&self, container: &Container, tid: &TransactionId) -> Result<String> {
        let (out, _err) = self.get_logs(container.container_id(), tid).await?;
        Ok(out)
    }
}

#[tonic::async_trait]
impl ContainerIsolationService for DockerIsolation {
    fn backend(&self) -> Vec<Isolation> {
        vec![Isolation::DOCKER]
    }

    /// creates and starts the entrypoint for a container based on the given image
    /// Run inside the specified namespace
    /// returns a new, unique ID representing it
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, fqdn, image_name, parallel_invokes, _namespace, mem_limit_mb, cpus), fields(tid=%tid)))]
    async fn run_container(
        &self,
        fqdn: &str,
        image_name: &str,
        parallel_invokes: u32,
        _namespace: &str,
        mem_limit_mb: MemSizeMb,
        cpus: u32,
        reg: &Arc<RegisteredFunction>,
        iso: Isolation,
        compute: Compute,
        device_resource: Option<Arc<crate::services::resources::gpu::GPU>>,
        tid: &TransactionId,
    ) -> Result<Container> {
        if !iso.eq(&Isolation::DOCKER) {
            anyhow::bail!("Only supports docker Isolation, now {:?}", iso);
        }
        let cid = format!("{}-{}", fqdn, GUID::rand());
        let port = free_local_port()?;
        let gunicorn_args = format!("GUNICORN_CMD_ARGS=--bind 0.0.0.0:{}", port);
        let cpu_arg = cpus.to_string();
        let port_args = format!("{}:{}", port, port);
        let il_port = format!("__IL_PORT={}", port);
        let memory_arg = format!("{}MB", mem_limit_mb);

        let mut args = vec![
            "--name",
            &cid,
            "-e",
            &gunicorn_args,
            "-e",
            &il_port,
            "--cpus",
            cpu_arg.as_str(),
            "--memory",
            &memory_arg,
            "-e",
            "__IL_HOST=0.0.0.0",
            "-p",
            &port_args,
        ];
        let gpu;
        let mps_thread;
        if let Some(device) = device_resource.as_ref() {
            gpu = format!("device={}", device.gpu_uuid);
            args.push("--gpus");
            args.push(gpu.as_str());

            if let Some(gpu_config) = self.config.gpu_resource.as_ref() {
                if gpu_config.is_tegra.unwrap_or(false) {
                    args.push("--runtime");
                    args.push("nvidia");
                }
            }

            if self.config.gpu_resource.as_ref().map_or(false, |c| c.mps_enabled()) {
                args.push("--ipc=host");
                args.push("-e");
                mps_thread = format!("CUDA_MPS_ACTIVE_THREAD_PERCENTAGE={}", device.thread_pct);
                args.push(mps_thread.as_str());
                args.push("-v");
                args.push("/tmp/nvidia-mps:/tmp/nvidia-mps");
            }
            if self
                .config
                .gpu_resource
                .as_ref()
                .map_or(false, |c| c.driver_hook_enabled())
            {
                args.push("-e");
                args.push("LD_PRELOAD=/app/libgpushare.so");
            }
        }

        let permit = match &self.creation_sem {
            Some(sem) => match sem.acquire().await {
                Ok(p) => {
                    debug!(tid=%tid, "Acquired docker creation semaphore");
                    Some(p)
                }
                Err(e) => {
                    bail_error!(error=%e, tid=%tid, "Error trying to acquire docker creation semaphore");
                }
            },
            None => None,
        };

        let time = format!("{}", self.limits_config.timeout_sec);
        let proc_args = vec!["server:app", "-w", "1", "--timeout", time.as_str()];
        // let proc_args = format!("server:app -w 1 --timeout {}", self.limits_config.timeout_sec);
        self.docker_run(args, image_name, cid.as_str(), Some(proc_args), tid, None)
            .await?;
        drop(permit);
        unsafe {
            let c = DockerContainer::new(
                cid,
                port,
                "0.0.0.0".to_string(),
                std::num::NonZeroU32::new_unchecked(parallel_invokes),
                fqdn,
                reg,
                self.limits_config.timeout_sec,
                ContainerState::Cold,
                compute,
                device_resource,
                tid,
            )?;
            Ok(Arc::new(c))
        }
    }

    /// Removed the specified container in the containerd namespace
    async fn remove_container(&self, container: Container, _ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
      let options = RemoveContainerOptions{force:true, v:true, link:false};
      match self.docker_api.remove_container(container.container_id().as_str(), Some(options)).await {
          Ok(_) => Ok(()),
          Err(e) => bail_error!(tid=%tid, error=%e, "Failed to remove Docker container"),
      }
    }

    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        _fqdn: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        if self.pulled_images.contains(&rf.image_name) {
            return Ok(());
        }

        let options = Some(CreateImageOptions{
          from_image: rf.image_name.as_str(),
          ..Default::default()
        });
        let mut stream = self.docker_api.create_image(options, None, None);
        while let Some(res) = stream.next().await {
            match res {
                Ok(_) => (),
                Err(e) => bail_error!(tid=%tid, error=%e, "Failed to pull image"),
            }
        }
        info!(tid=%tid, name=%rf.image_name, "Docker image pulled successfully");
        self.pulled_images.insert(rf.image_name.clone());
        Ok(())
    }

    async fn clean_containers(
        &self,
        _ctd_namespace: &str,
        _self_src: Arc<dyn ContainerIsolationService>,
        tid: &TransactionId,
    ) -> Result<()> {
        let options = ListContainersOptions { all: true, limit: None, size: false, filters: HashMap::from_iter(vec![("label", vec![OWNER_TAG])]) };
        let list = match self.docker_api.list_containers(Some(options)).await {
            Ok(l) => l,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to list Docker containers"),
        };
        for container in list {
            if let Some(id) = container.id {
                  let options = RemoveContainerOptions{force:true, v:true, link:false};
                  match self.docker_api.remove_container(&id, Some(options)).await {
                      Ok(_) => (),
                      Err(e) => error!(tid=%tid, error=%e, "Failed to remove Docker container"),
                  }
            };
        }
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container, timeout_ms), fields(tid=%tid)))]
    async fn wait_startup(&self, container: &Container, timeout_ms: u64, tid: &TransactionId) -> Result<()> {
        let start = SystemTime::now();
        loop {
            match self.get_logs(container.container_id(), tid).await {
                Ok((_out, err)) => {
                    // stderr was written to, gunicorn server is either up or crashed
                    if err.contains("Booting worker with pid") {
                        break;
                    }
                }
                Err(e) => {
                    bail_error!(tid=%tid, container_id=%container.container_id(), error=%e, "Timeout while reading inotify events for docker container")
                }
            };
            if start.elapsed()?.as_millis() as u64 >= timeout_ms {
                let (stdout, stderr) = self.get_logs(container.container_id(), tid).await?;
                // let stdout = self.read_stdout(container, tid).await;
                // let stderr = self.read_stderr(container, tid).await;
                if !stderr.is_empty() {
                    warn!(tid=%tid, container_id=%&container.container_id(), "Timeout waiting for docker container start, but stderr was written to?");
                    return Ok(());
                }
                bail_error!(tid=%tid, container_id=%container.container_id(), stdout=%stdout, stderr=%stderr, "Timeout while monitoring logs for docker container");
            }
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    async fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
        debug!(tid=%tid, container_id=%container.container_id(), "Updating memory usage for container");
        let cast_container = match crate::services::containers::structs::cast::<DockerContainer>(container) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=%tid, error=%e, "Error casting container to DockerContainer");
                return container.get_curr_mem_usage();
            }
        };
        let options = StatsOptions {
          stream: false,
          one_shot: false
        };
        let mut stream = self.docker_api.stats(cast_container.container_id.as_str(), Some(options));
        while let Some(res) = stream.next().await {
          match res {
              Ok(stats) => {
                if let Some(usage_bytes) = stats.memory_stats.usage {
                  let usage_mb: MemSizeMb = (usage_bytes / (1024*1024)) as MemSizeMb;
                  container.set_curr_mem_usage(usage_mb);
                  return usage_mb;
                }
              },
              Err(e) => {
                error!(tid=%tid, error=%e, "Failed to query stats");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
              },
          }
        }
        warn!(tid=%tid, "Fell out of bottom of stats stream loop");
        container.get_curr_mem_usage()
    }

    async fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
        match self.get_stdout(container, tid).await {
            Ok(out) => out,
            Err(_) => "".to_string(),
        }
    }
    async fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
        match self.get_stderr(container, tid).await {
            Ok(err) => err,
            Err(_) => "".to_string(),
        }
    }
}
impl crate::services::containers::structs::ToAny for DockerIsolation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
