use self::dockerstructs::DockerContainer;
use super::{structs::Container, ContainerIsolationService};
use crate::{
    services::{containers::structs::ContainerState, registration::RegisteredFunction},
    worker_api::worker_config::{ContainerResourceConfig, FunctionLimits},
};
use anyhow::Result;
use dashmap::DashSet;
use guid_create::GUID;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
    utils::{execute_cmd, port::free_local_port},
};
use std::{sync::Arc, time::SystemTime};
use tracing::{debug, error, info, trace, warn};

pub mod dockerstructs;

#[derive(Debug)]
#[allow(unused)]
pub struct DockerIsolation {
    config: Arc<ContainerResourceConfig>,
    limits_config: Arc<FunctionLimits>,
    creation_sem: Option<tokio::sync::Semaphore>,
    pulled_images: DashSet<String>,
}

impl DockerIsolation {
    pub fn supported(tid: &TransactionId) -> bool {
        let args = vec!["ps"];
        match execute_cmd("/usr/bin/docker", &args, None, tid) {
            Ok(out) => out.status.success(),
            Err(_) => false,
        }
    }

    pub fn new(config: Arc<ContainerResourceConfig>, limits_config: Arc<FunctionLimits>) -> Self {
        let sem = match config.concurrent_creation {
            0 => None,
            i => Some(tokio::sync::Semaphore::new(i as usize)),
        };
        DockerIsolation {
            config,
            limits_config,
            creation_sem: sem,
            pulled_images: DashSet::new(),
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
        Ok((
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        ))
    }

    fn get_stderr(&self, container: &Container, tid: &TransactionId) -> Result<String> {
        let (_out, err) = self.get_logs(container, tid)?;
        Ok(err)
    }

    fn get_stdout(&self, container: &Container, tid: &TransactionId) -> Result<String> {
        let (out, _err) = self.get_logs(container, tid)?;
        Ok(out)
    }

    /// example input: "1.366GiB / 2.5GiB"
    /// returns Ok(1398)
    fn parse_mem(input: std::borrow::Cow<str>) -> Result<MemSizeMb> {
        let mut end_of_num: usize = 0;
        let mut end_of_scale: usize = 0;
        for (i, c) in input.chars().enumerate() {
            if c.is_digit(10) || c == '.' {
                end_of_num = i + c.len_utf8();
            }
            if c == ' ' {
                end_of_scale = i;
                break;
            }
        }
        if end_of_num > input.len() {
            anyhow::bail!("End of number {} is somehow greater that input '{}'", end_of_num, input)
        }
        if end_of_num > input.len() {
            anyhow::bail!(
                "End of scale {} is somehow greater that input '{}'",
                end_of_scale,
                input
            )
        }
        let number = &input[..end_of_num];
        let scale = &input[end_of_num..end_of_scale];
        let parsed = match number.parse::<f64>() {
            Ok(p) => p,
            Err(_) => anyhow::bail!("Unable to parse '{}'", number),
        };
        let scale = match scale {
            "GiB" => 1024.0,
            "MiB" => 1.0,
            "KiB" => 1.0 / 1024.0,
            unknown => anyhow::bail!("Memory scale '{}' had an unsupported size format", unknown),
        };
        Ok(f64::ceil(parsed * scale) as MemSizeMb)
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
        fqdn: &String,
        image_name: &String,
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
        let gpu = match device_resource.as_ref() {
            Some(g) => Some(format!("device={}", g.gpu_uuid)),
            None => None,
        };
        let memory_arg = format!("{}MB", mem_limit_mb);

        let mut args = vec![
            "run",
            "--detach",
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
            "--label",
            "owner=iluvatar_worker",
            "--cpus",
            "1",
            "-p",
            &port_args,
        ];
        if let Some(dev) = gpu.as_ref() {
            args.push("--gpus");
            args.push(dev);
        }
        args.push(image_name);
        args.push("-w 1");

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

        let output = execute_cmd("/usr/bin/docker", &args, None, tid)?;
        if let Some(status) = output.status.code() {
            if status != 0 {
                bail_error!(tid=%tid, status=status, output=?output, "Failed to create docker container with exit code");
            }
        } else {
            bail_error!(tid=%tid, output=?output, "Failed to create docker container with no exit code");
        }
        drop(permit);
        debug!(tid=%tid, name=%image_name, containerid=%cid, output=?output, "Docker container started successfully");
        info!(tid=%tid, name=%image_name, containerid=%cid, "Docker container started successfully");
        unsafe {
            let c = DockerContainer::new(
                cid,
                port,
                "0.0.0.0".to_string(),
                std::num::NonZeroU32::new_unchecked(parallel_invokes),
                &fqdn,
                &reg,
                self.limits_config.timeout_sec,
                ContainerState::Cold,
                compute,
                device_resource,
            )?;
            Ok(Arc::new(c))
        }
    }

    /// Removed the specified container in the containerd namespace
    async fn remove_container(&self, container: Container, _ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
        let output = execute_cmd(
            "/usr/bin/docker",
            &vec!["rm", "--force", container.container_id().as_str()],
            None,
            tid,
        )?;
        if let Some(status) = output.status.code() {
            if status != 0 {
                bail_error!(tid=%tid, container_id=%container.container_id(), status=status, output=?output, "Failed to remove docker container with exit code");
            }
        } else {
            bail_error!(tid=%tid, container_id=%container.container_id(), output=?output, "Failed to remove docker container with no exit code");
        }
        Ok(())
    }

    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        _fqdn: &String,
        tid: &TransactionId,
    ) -> Result<()> {
        if self.pulled_images.contains(&rf.image_name) {
            return Ok(());
        }

        let output = execute_cmd("/usr/bin/docker", &vec!["pull", rf.image_name.as_str()], None, tid)?;
        if let Some(status) = output.status.code() {
            if status != 0 {
                bail_error!(tid=%tid, status=status, output=?output, "Failed to pull docker image with exit code");
            }
        } else {
            bail_error!(tid=%tid, output=?output, "Failed to pull docker image with no exit code");
        }
        trace!(tid=%tid, name=%rf.image_name, output=?output, "Docker image pulled successfully");
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
        let output = execute_cmd(
            "/usr/bin/docker",
            &vec!["ps", "--filter", "label=owner=iluvatar_worker", "-q"],
            None,
            tid,
        )?;
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
                Ok((_out, err)) => {
                    // stderr was written to, gunicorn server is either up or crashed
                    if err.len() > 0 {
                        break;
                    }
                }
                Err(e) => {
                    bail_error!(tid=%tid, container_id=%container.container_id(), error=%e, "Timeout while reading inotify events for docker container")
                }
            };
            if start.elapsed()?.as_millis() as u64 >= timeout_ms {
                let stdout = self.read_stdout(&container, tid);
                let stderr = self.read_stderr(&container, tid);
                if stderr.len() > 0 {
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
    fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
        debug!(tid=%tid, container_id=%container.container_id(), "Updating memory usage for container");
        let cast_container = match crate::services::containers::structs::cast::<DockerContainer>(&container, tid) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=%tid, error=%e, "Error casting container to DockerContainer");
                return container.get_curr_mem_usage();
            }
        };
        let output = match execute_cmd(
            "/usr/bin/docker",
            &vec![
                "stats",
                "--no-stream",
                "--format",
                "{{.MemUsage}}",
                cast_container.container_id.as_str(),
            ],
            None,
            tid,
        ) {
            Ok(o) => o,
            Err(e) => {
                error!(tid=%tid, error=%e, "Failed to run 'docker stats' with error");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
            }
        };
        if let Some(status) = output.status.code() {
            if status != 0 {
                error!(tid=%tid, status=status, output=?output, "Failed to run 'docker stats' with exit code");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            match Self::parse_mem(stdout) {
                Ok(m) => {
                    container.set_curr_mem_usage(m);
                    return m;
                }
                Err(e) => {
                    error!(tid=%tid, error=%e, "Failed to parse memory value");
                    container.mark_unhealthy();
                    return container.get_curr_mem_usage();
                }
            };
        } else {
            error!(tid=%tid, output=?output, "Failed to run 'docker stats' with no exit code");
            return container.get_curr_mem_usage();
        }
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
impl crate::services::containers::structs::ToAny for DockerIsolation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod docker_tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("131.9MiB / 3.023GiB\n", 132)]
    #[case("131MiB / 3.023GiB\n", 131)]
    #[case("131.MiB / 3.023GiB\n", 131)]
    #[case("1.862GiB / 2GiB\n", 1907)]
    #[case("100KiB / 2GiB\n", 1)]
    #[case("1025KiB / 2GiB\n", 2)]
    #[case("1GiB / 2GiB\n", 1024)]
    #[case("0.5GiB / 2GiB\n", 512)]
    fn parse_mem_works(#[case] input: &str, #[case] expected: MemSizeMb) {
        assert_eq!(
            DockerIsolation::parse_mem(std::borrow::Cow::Borrowed(input)).unwrap(),
            expected
        );
    }

    #[rstest]
    #[case("131.9Mi / 3.023GiB\n")]
    #[case("131iB / 3.023GiB\n")]
    #[case("MiB / 3.023GiB\n")]
    fn parse_mem_errors(#[case] input: &str) {
        match DockerIsolation::parse_mem(std::borrow::Cow::Borrowed(input)) {
            Ok(r) => panic!("Case should fail but got '{}'", r),
            Err(_) => (),
        }
    }
}
