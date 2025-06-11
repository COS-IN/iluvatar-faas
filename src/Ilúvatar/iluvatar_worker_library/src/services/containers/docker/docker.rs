use self::dockerstructs::DockerContainer;
use super::{structs::Container, ContainerIsolationService};
use crate::services::registration::RunFunction;
use crate::services::resources::gpu::GPU;
use crate::{
    services::{containers::structs::ContainerState, registration::RegisteredFunction},
    worker_api::worker_config::{ContainerResourceConfig, FunctionLimits},
};
use anyhow::Result;
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptions, ListContainersOptions, LogsOptions, RemoveContainerOptions,
    StartContainerOptions, StatsOptions,
};
use bollard::secret::{ContainerCreateBody, ContainerSummaryStateEnum};
use bollard::Docker;
use bollard::{
    auth::DockerCredentials,
    models::{DeviceRequest, HostConfig, PortBinding},
};
use dashmap::DashSet;
use futures::StreamExt;
use guid_create::GUID;
use iluvatar_library::clock::now;
use iluvatar_library::types::{err_val, ContainerServer, ResultErrorVal};
use iluvatar_library::utils::file::{container_path, make_paths};
use iluvatar_library::{
    bail_error, bail_error_value, error_value,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
    utils::port::free_local_port,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

pub mod dockerstructs;

const OWNER_TAG: &str = "owner=iluvatar_worker";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
/// Authentication for a specific Docker repository
pub struct DockerAuth {
    pub username: String,
    pub password: String,
    pub repository: String,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
/// Optional configuration to modify or pass through to Docker
pub struct DockerConfig {
    pub auth: Option<DockerAuth>,
    #[serde(default)]
    /// Avoid pulling images if a matching <image:tag> is found.
    /// Pulls it if missing.
    /// Can skip pulling updated version of a tag, but saves time & avoids rate limiting.
    pub avoid_pull: bool,
}

#[allow(unused)]
#[derive(iluvatar_library::ToAny)]
pub struct DockerIsolation {
    config: Arc<ContainerResourceConfig>,
    limits_config: Arc<FunctionLimits>,
    creation_sem: Option<tokio::sync::Semaphore>,
    pulled_images: DashSet<String>,
    docker_api: Docker,
    docker_config: Option<DockerConfig>,
}
pub type BollardPortBindings = Option<HashMap<String, Option<Vec<PortBinding>>>>;
impl DockerIsolation {
    pub async fn supported(tid: &TransactionId) -> bool {
        let docker = match Docker::connect_with_socket_defaults() {
            Ok(d) => d,
            Err(e) => {
                warn!(tid=tid, error=%e, "Failed to connect to docker");
                return false;
            },
        };
        match docker.ping().await {
            Ok(_) => true,
            Err(e) => {
                warn!(tid=tid, error=?e, "Failed to query docker version");
                false
            },
        }
    }

    pub fn new(
        config: Arc<ContainerResourceConfig>,
        limits_config: Arc<FunctionLimits>,
        docker_config: Option<DockerConfig>,
        tid: &TransactionId,
    ) -> Result<Self> {
        let docker = match Docker::connect_with_socket_defaults() {
            Ok(d) => d,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to connect to docker"),
        };
        let sem = match config.concurrent_creation {
            0 => None,
            i => Some(tokio::sync::Semaphore::new(i as usize)),
        };
        Ok(DockerIsolation {
            config,
            limits_config,
            docker_config,
            creation_sem: sem,
            pulled_images: DashSet::new(),
            docker_api: docker,
        })
    }

    pub async fn docker_run(
        &self,
        tid: &TransactionId,
        container_id: &str,
        mut env: Vec<String>,
        reg: &RegisteredFunction,
        device_resource: &Option<GPU>,
        ports: BollardPortBindings,
        host_config: Option<HostConfig>,
        entrypoint: Option<Vec<String>>,
    ) -> Result<()> {
        let mut host_config = host_config.unwrap_or_default();
        host_config.cpu_shares = Some((reg.cpus * 1024) as i64);
        host_config.memory = Some(reg.memory * 1024 * 1024);
        let exposed_ports: Option<HashMap<String, HashMap<(), ()>>> = match ports.as_ref() {
            Some(p) => {
                let mut exposed = HashMap::new();
                for (port, _) in p.iter() {
                    exposed.insert(port.clone(), HashMap::new());
                }
                Some(exposed)
            },
            None => None,
        };
        let ctr_dir = container_path(container_id);
        make_paths(&ctr_dir, tid)?;
        let mut volumes = vec![];
        let mut device_requests = vec![];

        if reg.container_server == ContainerServer::UnixSocket {
            volumes.push(format!("{}:/iluvatar/sockets", ctr_dir.to_string_lossy()));
        }

        match &reg.run_info {
            RunFunction::Runtime {
                packages_dir, main_dir, ..
            } => {
                volumes.push(format!("{}:/app/packages", packages_dir));
                volumes.push(format!("{}:/app/main", main_dir));
                env.push("PYTHONPATH=/app/main:/app/packages".to_string());
            },
            _ => {},
        };

        if let Some(device) = device_resource.as_ref() {
            info!(tid=tid, container_id=%container_id, "Container will get a GPU");
            device_requests.push(DeviceRequest {
                driver: Some("".into()),
                count: None,
                device_ids: Some(vec![device.gpu_uuid.clone()]),
                capabilities: Some(vec![vec!["gpu".into()]]),
                options: Some(HashMap::new()),
            });

            if let Some(gpu_config) = self.config.gpu_resource.as_ref() {
                if gpu_config.is_tegra.unwrap_or(false) {
                    host_config.runtime = Some("nvidia".to_owned());
                }
            }

            if self.config.gpu_resource.as_ref().is_some_and(|c| c.mps_enabled()) {
                info!(tid=tid, container_id=%container_id, threads=device.thread_pct, memory=device.allotted_mb, "Container running inside MPS context");
                host_config.ipc_mode = Some("host".to_owned());
                let mps_thread = format!("CUDA_MPS_ACTIVE_THREAD_PERCENTAGE={}", device.thread_pct);
                let mps_mem = format!("CUDA_MPS_PINNED_DEVICE_MEM_LIMIT={}MB", device.allotted_mb);
                env.push(mps_thread);
                env.push(mps_mem);
                volumes.push("/tmp/nvidia-mps:/tmp/nvidia-mps".to_owned());
            }
            if self
                .config
                .gpu_resource
                .as_ref()
                .is_some_and(|c| c.driver_hook_enabled())
            {
                env.push("LD_PRELOAD=/app/libgpushare.so".to_owned());
            }
        }
        match host_config.binds.as_mut() {
            Some(binds) => binds.extend(volumes),
            None => host_config.binds = Some(volumes),
        };
        match host_config.device_requests.as_mut() {
            Some(cfg_device_requests) => cfg_device_requests.extend(device_requests),
            None => host_config.device_requests = Some(device_requests),
        };
        match host_config.port_bindings.as_mut() {
            Some(port_bindings) => {
                if let Some(ports) = ports {
                    port_bindings.extend(ports)
                }
            },
            None => host_config.port_bindings = ports,
        };
        let options = CreateContainerOptionsBuilder::new().name(container_id).build();
        let config = ContainerCreateBody {
            labels: Some(HashMap::from([("owner".to_owned(), "iluvatar_worker".to_owned())])),
            image: Some(reg.image_name.to_owned()),
            host_config: Some(host_config),
            env: Some(env),
            exposed_ports,
            entrypoint,
            ..Default::default()
        };
        debug!(tid=tid, container_id=%container_id, config=?config, "Creating container");
        match self.docker_api.create_container(Some(options), config).await {
            Ok(_) => (),
            Err(e) => bail_error!(tid=tid, error=%e, "Error creating container"),
        };
        debug!(tid=tid, container_id=%container_id, "Container created");

        match self
            .docker_api
            .start_container(container_id, None::<StartContainerOptions>)
            .await
        {
            Ok(_) => (),
            Err(e) => bail_error!(tid=tid, error=%e, "Error starting container"),
        };
        debug!(tid=tid, container_id=%container_id, "Container started");
        Ok(())
    }

    /// Get the stdout and stderr of a container
    pub async fn get_logs(&self, container_id: &str, tid: &TransactionId) -> Result<(String, String)> {
        let options = LogsOptions {
            stdout: true,
            stderr: true,
            ..Default::default()
        };
        let mut stream = self.docker_api.logs(container_id, Some(options));
        let mut stdout = "".to_string();
        let mut stderr = "".to_string();
        while let Some(res) = stream.next().await {
            match res {
                Ok(r) => match r {
                    bollard::container::LogOutput::StdErr { message } => {
                        stderr = String::from_utf8_lossy(&message).to_string()
                    },
                    bollard::container::LogOutput::StdOut { message } => {
                        stdout = String::from_utf8_lossy(&message).to_string()
                    },
                    _ => (),
                },
                Err(e) => bail_error!(tid=tid, error=%e, "Failed to get Docker logs"),
            }
        }
        Ok((stdout, stderr))
    }

    async fn get_stderr(&self, container: &Container, tid: &TransactionId) -> Result<String> {
        let (_out, err) = self.get_logs(container.container_id(), tid).await?;
        Ok(err)
    }

    async fn get_stdout(&self, container: &Container, tid: &TransactionId) -> Result<String> {
        let (out, _err) = self.get_logs(container.container_id(), tid).await?;
        Ok(out)
    }

    async fn dependencies(&self, rf: &RegisteredFunction, tid: &TransactionId) -> Result<()> {
        match &rf.run_info {
            RunFunction::Runtime { .. } => {
                let env = vec![];
                let cid = format!("{}-prep", rf.fqdn);
                let entrypoint = [
                    "python3",
                    "-m",
                    "pip",
                    "install",
                    "--ignore-installed",
                    "--progress-bar",
                    "off",
                    "--compile",
                    "--target",
                    "/app/packages",
                    "-r",
                    "/app/main/reqs.txt",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect();

                if let Err(e) = self
                    .docker_run(tid, cid.as_str(), env, rf, &None, None, None, Some(entrypoint))
                    .await
                {
                    bail_error!(error=%e, tid=tid, "failed preparing code");
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                let mut list_container_filters = HashMap::new();
                list_container_filters.insert(String::from("name"), vec![cid.clone()]);
                debug!(tid = tid, "waiting for package install to finish");
                loop {
                    let containers = &self
                        .docker_api
                        .list_containers(Some(
                            bollard::query_parameters::ListContainersOptionsBuilder::default()
                                .filters(&list_container_filters)
                                .all(true)
                                .build(),
                        ))
                        .await?;
                    for c in containers {
                        match c.state {
                            Some(s) => match s {
                                ContainerSummaryStateEnum::EXITED => {
                                    self.remove_container_intern(&cid, tid).await?;
                                    return Ok(());
                                },
                                ContainerSummaryStateEnum::REMOVING | ContainerSummaryStateEnum::DEAD => {
                                    let (stdout, stderr) = self.get_logs(&cid, tid).await?;
                                    self.remove_container_intern(&cid, tid).await?;
                                    bail_error!(
                                        tid = tid,
                                        stdout = stdout,
                                        stderr = stderr,
                                        "failed installing dependencies"
                                    )
                                },
                                _ => tokio::time::sleep(tokio::time::Duration::from_secs(1)).await,
                            },
                            _ => (),
                        }
                    }
                }
            },
            _ => Ok(()),
        }
    }

    async fn remove_container_intern(&self, container_id: &str, tid: &TransactionId) -> Result<()> {
        let options = RemoveContainerOptions {
            force: true,
            v: true,
            link: false,
        };
        match self.docker_api.remove_container(container_id, Some(options)).await {
            Ok(_) => Ok(()),
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to remove Docker container"),
        }
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
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, _namespace, iso, compute, device_resource), fields(tid=tid)))]
    async fn run_container(
        &self,
        _namespace: &str,
        reg: &Arc<RegisteredFunction>,
        iso: Isolation,
        compute: Compute,
        device_resource: Option<GPU>,
        tid: &TransactionId,
    ) -> ResultErrorVal<Container, Option<GPU>> {
        if !iso.eq(&Isolation::DOCKER) {
            error_value!("Only supports docker Isolation, now {:?}", iso, device_resource);
        }
        let mut env = vec![];
        let cid = format!("{}-{}", reg.fqdn, GUID::rand());
        let port = match free_local_port() {
            Ok(p) => p,
            Err(e) => return err_val(e, device_resource),
        };
        let gunicorn_args = format!(
            "GUNICORN_CMD_ARGS=--workers=1 --timeout={} --bind=0.0.0.0:{}",
            &self.limits_config.timeout_sec, port
        );
        env.push(gunicorn_args);
        let mut ports = HashMap::new();
        ports.insert(
            format!("{}/tcp", port),
            Some(vec![PortBinding {
                host_ip: Some("".to_string()),
                host_port: Some(port.to_string()),
            }]),
        );
        env.push(format!("__IL_PORT={}", port));
        env.push(format!("__IL_SOCKET={}", "/iluvatar/sockets/sock"));

        let permit = match &self.creation_sem {
            Some(sem) => match sem.acquire().await {
                Ok(p) => {
                    debug!(tid = tid, "Acquired docker creation semaphore");
                    Some(p)
                },
                Err(e) => {
                    bail_error_value!(error=%e, tid=tid, "Error trying to acquire docker creation semaphore", device_resource);
                },
            },
            None => None,
        };

        info!(tid = tid, cid = cid, "launching container");
        if let Err(e) = self
            .docker_run(tid, cid.as_str(), env, reg, &device_resource, Some(ports), None, None)
            .await
        {
            bail_error_value!(error=%e, tid=tid, "Error trying to acquire docker creation semaphore", device_resource);
        };

        drop(permit);
        unsafe {
            let c = match DockerContainer::new(
                cid,
                port,
                "0.0.0.0".to_string(),
                std::num::NonZeroU32::new_unchecked(reg.parallel_invokes),
                reg,
                self.limits_config.timeout_sec,
                ContainerState::Cold,
                compute,
                device_resource,
                tid,
            )
            .await
            {
                Ok(c) => c,
                Err((e, d)) => return err_val(e, d),
            };
            Ok(Arc::new(c))
        }
    }

    /// Removed the specified container in the containerd namespace
    async fn remove_container(&self, container: Container, _ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
        self.remove_container_intern(container.container_id(), tid).await
        // let options = RemoveContainerOptions {
        //     force: true,
        //     v: true,
        //     link: false,
        // };
        // match self
        //     .docker_api
        //     .remove_container(container.container_id().as_str(), Some(options))
        //     .await
        // {
        //     Ok(_) => Ok(()),
        //     Err(e) => bail_error!(tid=tid, error=%e, "Failed to remove Docker container"),
        // }
    }

    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        _namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        debug!(tid = tid, "prepare_function_registration");
        if self.pulled_images.contains(&rf.image_name) {
            debug!(tid = tid, "image exists, skipping");
            return Ok(());
        }

        let auth = match &self.docker_config {
            Some(cfg) => {
                if cfg.avoid_pull {
                    let image_name_no_hub = rf.image_name.split('/').skip(1).collect::<Vec<&str>>().join("/");
                    debug!(tid = tid, query = image_name_no_hub, "querying images");
                    let list = bollard::query_parameters::ListImagesOptionsBuilder::default()
                        .filters(&HashMap::from_iter([(
                            "reference".to_string(),
                            vec![image_name_no_hub.clone()],
                        )]))
                        .build();

                    match self.docker_api.list_images(Some(list)).await {
                        Ok(ls) => {
                            for image in ls {
                                for tag in &image.repo_tags {
                                    if tag == &image_name_no_hub {
                                        info!(tid=tid, image=?image, "image found, skipping pull");
                                        self.dependencies(rf, tid).await?;
                                        return Ok(());
                                    }
                                }
                            }
                        },
                        Err(e) => warn!(tid=tid, error=%e, "Failed to list docker images"),
                    };
                }
                match &cfg.auth {
                    Some(a) if rf.image_name.starts_with(a.repository.as_str()) => Some(DockerCredentials {
                        username: Some(a.username.clone()),
                        password: Some(a.password.clone()),
                        ..Default::default()
                    }),
                    _ => None,
                }
            },
            None => None,
        };

        let options = Some(CreateImageOptions {
            from_image: Some(rf.image_name.clone()),
            ..Default::default()
        });

        let mut stream = self.docker_api.create_image(options, None, auth);
        while let Some(res) = stream.next().await {
            match res {
                Ok(inf) => debug!(tid=tid, info=?inf, "pull info update"),
                Err(e) => bail_error!(tid=tid, error=%e, "Failed to pull image"),
            }
        }
        info!(tid=tid, name=%rf.image_name, "Docker image pulled successfully");
        self.pulled_images.insert(rf.image_name.clone());
        self.dependencies(rf, tid).await?;
        Ok(())
    }

    async fn clean_containers(
        &self,
        _ctd_namespace: &str,
        _self_src: Arc<dyn ContainerIsolationService>,
        tid: &TransactionId,
    ) -> Result<()> {
        let options = ListContainersOptions {
            all: true,
            limit: None,
            size: false,
            filters: Some(HashMap::from_iter(vec![(
                "label".to_owned(),
                vec![OWNER_TAG.to_owned()],
            )])),
        };
        let list = match self.docker_api.list_containers(Some(options)).await {
            Ok(l) => l,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to list Docker containers"),
        };
        for container in list {
            if let Some(id) = container.id {
                let options = RemoveContainerOptions {
                    force: true,
                    v: true,
                    link: false,
                };
                match self.docker_api.remove_container(&id, Some(options)).await {
                    Ok(_) => (),
                    Err(e) => error!(tid=tid, error=%e, "Failed to remove Docker container"),
                }
            };
        }
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container, timeout_ms), fields(tid=tid)))]
    async fn wait_startup(&self, container: &Container, timeout_ms: u64, tid: &TransactionId) -> Result<()> {
        let start = now();
        loop {
            match self.get_logs(container.container_id(), tid).await {
                Ok((_out, err)) => {
                    // stderr will have container startup magic string
                    if err.contains("MGK_GUN_READY_KMG") {
                        break;
                    }
                },
                Err(e) => {
                    bail_error!(tid=tid, container_id=%container.container_id(), error=%e, "Timeout while reading inotify events for docker container")
                },
            };
            if start.elapsed().as_millis() as u64 >= timeout_ms {
                let (stdout, stderr) = self.get_logs(container.container_id(), tid).await?;
                if !stderr.is_empty() {
                    warn!(tid=tid, container_id=%&container.container_id(), "Timeout waiting for docker container start, but stderr was written to?");
                    return Ok(());
                }
                bail_error!(tid=tid, container_id=%container.container_id(), stdout=%stdout, stderr=%stderr, "Timeout while monitoring logs for docker container");
            }
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container), fields(tid=tid)))]
    async fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
        debug!(tid=tid, container_id=%container.container_id(), "Updating memory usage for container");
        let cast_container = match crate::services::containers::structs::cast::<DockerContainer>(container) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=tid, error=%e, "Error casting container to DockerContainer");
                return container.get_curr_mem_usage();
            },
        };
        let options = StatsOptions {
            stream: false,
            one_shot: true,
        };
        let mut stream = self
            .docker_api
            .stats(cast_container.container_id.as_str(), Some(options));
        while let Some(res) = stream.next().await {
            match res {
                Ok(stats) => {
                    if let Some(usage_bytes) = stats.memory_stats.and_then(|s| s.usage.or(None)) {
                        let usage_mb: MemSizeMb = (usage_bytes / (1024 * 1024)) as MemSizeMb;
                        container.set_curr_mem_usage(usage_mb);
                        return usage_mb;
                    }
                },
                Err(e) => {
                    error!(tid=tid, error=%e, "Failed to query stats");
                    container.mark_unhealthy();
                    return container.get_curr_mem_usage();
                },
            }
        }
        warn!(tid=tid, container_id=%container.container_id(), "Fell out of bottom of stats stream loop");
        container.get_curr_mem_usage()
    }

    async fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
        self.get_stdout(container, tid).await.unwrap_or_else(|_| "".to_string())
    }
    async fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
        self.get_stderr(container, tid).await.unwrap_or_else(|_| "".to_string())
    }
}
