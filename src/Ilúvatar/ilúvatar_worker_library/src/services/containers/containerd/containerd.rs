use super::ContainerIsolationService;
use crate::services::containers::containerd::containerdstructs::{ContainerdContainer, Task};
use crate::services::containers::structs::{Container, ContainerState};
use crate::services::network::namespace_manager::NamespaceManager;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::worker_config::{ContainerResourceConfig, FunctionLimits};
use anyhow::Result;
use client::services::v1::container::Runtime;
use client::services::v1::snapshots::{snapshots_client::SnapshotsClient, PrepareSnapshotRequest};
use client::services::v1::Container as Containerd_Container;
use client::services::v1::{containers_client::ContainersClient, tasks_client::TasksClient};
use client::services::v1::{content_client::ContentClient, images_client::ImagesClient, version_client::VersionClient};
use client::services::v1::{
    CreateContainerRequest, CreateTaskRequest, DeleteContainerRequest, DeleteTaskRequest, KillRequest,
    ListContainersRequest, StartRequest, VersionResponse,
};
use client::services::v1::{GetImageRequest, ReadContentRequest};
use client::tonic::Code;
use client::types::Descriptor;
use client::with_namespace;
use containerd_client as client;
use containerd_client::tonic::{transport::Channel, Request};
use dashmap::DashMap;
use guid_create::GUID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::utils::{
    cgroup::cgroup_namespace,
    file::{temp_file_pth, touch, try_remove_pth},
    port::Port,
};
use iluvatar_library::{bail_error, transaction::TransactionId, types::MemSizeMb};
use inotify::{Inotify, WatchMask};
use oci_spec::image::{ImageConfiguration, ImageIndex, ImageManifest};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, error, info, warn};

pub mod containerdstructs;
const CONTAINERD_SOCK: &str = "/run/containerd/containerd.sock";

#[derive(Debug)]
pub struct ContainerdIsolation {
    channel: Option<Channel>,
    namespace_manager: Arc<NamespaceManager>,
    config: Arc<ContainerResourceConfig>,
    limits_config: Arc<FunctionLimits>,
    downloaded_images: Arc<DashMap<String, bool>>,
    creation_sem: Option<tokio::sync::Semaphore>,
}

/// A service to handle the low-level details of containerd container lifecycles:
///   creation, destruction, pulling images, etc
impl ContainerdIsolation {
    pub async fn supported(tid: &TransactionId) -> bool {
        let channel = match containerd_client::connect(CONTAINERD_SOCK).await {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=%tid, error=?e, "Failed to connect to containerd socket");
                return false;
            }
        };
        let mut client = VersionClient::new(channel);
        let _: client::tonic::Response<VersionResponse> = match client.version(()).await {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=%tid, error=?e, "Failed to query Containerd version");
                return false;
            }
        };
        true
    }

    pub fn new(
        ns_man: Arc<NamespaceManager>,
        config: Arc<ContainerResourceConfig>,
        limits_config: Arc<FunctionLimits>,
    ) -> ContainerdIsolation {
        let sem = match config.concurrent_creation {
            0 => None,
            i => Some(tokio::sync::Semaphore::new(i as usize)),
        };
        ContainerdIsolation {
            // this is threadsafe if we clone channel
            // https://docs.rs/tonic/0.4.0/tonic/transport/struct.Channel.html#multiplexing-requests
            channel: None,
            namespace_manager: ns_man,
            config,
            limits_config,
            downloaded_images: Arc::new(DashMap::new()),
            creation_sem: sem,
        }
    }

    /// connect to the containerd socket
    pub async fn connect(&mut self) -> Result<()> {
        if let Some(_) = &self.channel {
            Ok(())
        } else {
            let channel = containerd_client::connect(CONTAINERD_SOCK).await?;
            self.channel = Some(channel);
            Ok(())
        }
    }

    /// get the channel to the containerd socket
    fn channel(&self) -> Channel {
        self.channel
            .as_ref()
            .expect("Tried to access channel before opening connection!")
            .clone()
    }

    /// get the default container spec
    fn spec(
        &self,
        host_addr: &str,
        port: Port,
        mem_limit_mb: MemSizeMb,
        cpus: u32,
        net_ns_name: &String,
        container_id: &String,
    ) -> prost_types::Any {
        // one second of time, in microseconds
        let one_sec_in_us: u64 = 1 * 1000 * 1000;
        // https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md
        let spec = include_str!("../../../resources/container_spec.json");
        let spec = spec
            .to_string()
            .replace("$ROOTFS", "rootfs")
            .replace("$OUTPUT", "")
            .replace("$HOST_ADDR", host_addr)
            .replace("$PORT", &port.to_string())
            .replace("$NET_NS", &NamespaceManager::net_namespace(net_ns_name))
            .replace("\"$MEMLIMIT\"", &(mem_limit_mb * 1024 * 1024).to_string())
            //        .replace("\"$SWAPLIMIT\"", &(mem_limit_mb*1024*1024*2).to_string())
            // .replace("\"$CPUSHARES\"", &(cpus*1024).to_string())
            // a function with 1 cpu will have an equal number of
            .replace("\"$CPUQUOTA\"", &((cpus as u64) * one_sec_in_us).to_string())
            .replace("\"$CPUPERIOD\"", &one_sec_in_us.to_string())
            .replace("$INVOKE_TIMEOUT", &self.limits_config.timeout_sec.to_string())
            .replace("$CGROUPSPATH", &cgroup_namespace(container_id))
            .replace("$RESOLV_CONF", &NamespaceManager::resolv_conf_path());
        prost_types::Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
            value: spec.into_bytes(),
        }
    }

    async fn read_content(&self, namespace: &str, digest: String) -> Result<Vec<u8>> {
        let read_content_req = ReadContentRequest {
            digest,
            offset: 0,
            size: 0,
        };
        let mut cli = ContentClient::new(self.channel());

        match cli.read(with_namespace!(read_content_req, namespace)).await {
            Ok(rsp) => match rsp.into_inner().message().await {
                Ok(Some(rsp)) => Ok(rsp.data),
                Ok(None) => bail_error!("Did not get data reading message content {}", ""),
                Err(e) => bail_error!("failed to read content message because {}", e),
            },
            Err(e) => bail_error!("failed to read content because {}", e),
        }
    }

    /// get the mount points for a container's (id) snapshot base
    async fn load_mounts(
        &self,
        cid: &str,
        snapshot_base: &String,
        tid: &TransactionId,
    ) -> Result<Vec<containerd_client::types::Mount>> {
        let view_snapshot_req = PrepareSnapshotRequest {
            // https://github.com/containerd/containerd/tree/main/docs/snapshotters
            snapshotter: self.config.snapshotter.clone(),
            key: cid.to_owned(),
            parent: snapshot_base.clone(),
            labels: HashMap::new(),
        };
        let mut cli = SnapshotsClient::new(self.channel());
        let rsp = cli.prepare(with_namespace!(view_snapshot_req, "default")).await;
        if let Ok(rsp) = rsp {
            let rsp = rsp.into_inner();
            debug!(tid=%tid, container_id=%cid, mounts=?rsp.mounts, "got mounts");
            Ok(rsp.mounts)
        } else {
            bail_error!(tid=%tid, response=?rsp, "Failed to prepare snapshot and load mounts")
        }
    }

    async fn delete_task(
        &self,
        client: &mut TasksClient<Channel>,
        container_id: &String,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let start = SystemTime::now();
        let timeout = Duration::from_secs(10);
        loop {
            match self.try_delete_task(client, container_id, ctd_namespace, tid).await {
                Ok(_) => return Ok(()),
                Err(e2) => {
                    if start.elapsed()? > timeout {
                        bail_error!(tid=%tid, container_id=%container_id, error=%e2, "Deleting task in container failed");
                    }
                    // sleep a little and hope the process has terminated in that time
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            };
        }
    }

    /// Attempts to delete a task
    /// Sometimes this can fail if the internal process hasn't shut down yet (or wasn't killed at all)
    async fn try_delete_task(
        &self,
        client: &mut TasksClient<Channel>,
        container_id: &String,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let req = DeleteTaskRequest {
            container_id: container_id.clone(),
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = client.delete(req).await;
        match &resp {
            Ok(_) => {
                debug!(tid=%tid, response=?resp, "Delete task response");
                Ok(())
            }
            Err(e) => {
                match e.code() {
                    // task crashed and was removed
                    Code::NotFound => {
                        warn!(tid=%tid, container_id=%container_id, "Task for container was missing when it was attempted to be delete. Usually the process crashed");
                        Ok(())
                    }
                    _ => anyhow::bail!(
                        "[{}] Attempt to delete task in container '{}' failed with error: {}",
                        tid,
                        container_id,
                        e
                    ),
                }
            }
        }
    }

    async fn kill_task(
        &self,
        client: &mut TasksClient<Channel>,
        container_id: &String,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let req = KillRequest {
            container_id: container_id.clone(),
            // exec_id: container.task.pid.to_string(),
            exec_id: "".to_string(),
            signal: 9, // SIGKILL
            all: true,
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = client.kill(req).await;
        match &resp {
            Ok(_) => {}
            Err(e) => {
                if e.code() == Code::NotFound {
                    // task crashed and was removed
                    warn!(tid=%tid, container_id=%container_id, "Task for container was missing when it was attempted to be killed");
                } else {
                    bail_error!(tid=%tid, container_id=%container_id, error=%e, "Attempt to kill task in container failed");
                }
            }
        };
        debug!(tid=%tid, response=?resp, "Kill task response");
        Ok(())
    }

    async fn delete_containerd_container(
        &self,
        client: &mut ContainersClient<Channel>,
        container_id: &String,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let req = DeleteContainerRequest {
            id: container_id.clone(),
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = match client.delete(req).await {
            Ok(resp) => resp,
            Err(e) => bail_error!(tid=%tid, error=%e, "Delete container failed"),
        };
        debug!(tid=%tid, response=?resp, "Delete container response");
        Ok(())
    }

    fn delete_container_resources(&self, container_id: &String, tid: &TransactionId) {
        try_remove_pth(&self.stdin_pth(container_id), tid);
        try_remove_pth(&self.stdout_pth(container_id), tid);
        try_remove_pth(&self.stderr_pth(container_id), tid);
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    async fn remove_container_internal(
        &self,
        container_id: &String,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        info!(tid=%tid, container_id=%container_id, "Removing container");
        let mut client = TasksClient::new(self.channel());
        self.kill_task(&mut client, container_id, ctd_namespace, tid).await?;
        self.delete_task(&mut client, container_id, ctd_namespace, tid).await?;

        let mut client = ContainersClient::new(self.channel());
        self.delete_containerd_container(&mut client, container_id, ctd_namespace, tid)
            .await?;
        self.delete_container_resources(container_id, tid);

        debug!(tid=%tid, container_id=%container_id, "Container deleted");
        Ok(())
    }

    /// Read through an image's digest to find it's snapshot base
    async fn search_image_digest(&self, image: &String, namespace: &str, tid: &TransactionId) -> Result<String> {
        // Step 1. get image digest
        let get_image_req = GetImageRequest { name: image.into() };
        let mut cli = ImagesClient::new(self.channel());
        let rsp = match cli.get(with_namespace!(get_image_req, namespace)).await {
            Ok(rsp) => rsp.into_inner(),
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to get image"),
        };
        debug!(tid=%tid,response=?rsp, "image response");
        let (image_digest, media_type) = if let Some(image) = rsp.image {
            image
                .target
                .ok_or_else(|| anyhow::anyhow!("Could not find image digest"))
                .map(|v: Descriptor| (v.digest, v.media_type))?
        } else {
            anyhow::bail!("Could not find image")
        };

        debug!(tid=%tid, image=%image, digest=?image_digest, "got image digest");

        // Step 2. get image content manifests
        let content = self.read_content(namespace, image_digest).await?;

        let layer_item = match media_type.as_str() {
            "application/vnd.docker.distribution.manifest.list.v2+json" => {
                let config_index: ImageIndex = match serde_json::from_slice(&content) {
                    Ok(s) => s,
                    Err(e) => bail_error!(tid=%tid, error=%e, "JSON error getting ImageIndex"),
                };
                debug!(tid=%tid, index=?config_index, "config ImageIndex");

                let manifest_item = config_index
                    .manifests()
                    .iter()
                    .find(|file| match file.platform() {
                        Some(v) => v.architecture().to_string() == "amd64" && v.os().to_string() == "linux",
                        None => false,
                    })
                    .ok_or_else(|| anyhow::anyhow!("fail to load specific manifest"))?
                    .digest()
                    .to_owned();

                debug!(tid=%tid, manifest=?manifest_item, "Acquired manifest item");
                // Step 3. load image manifest from specific platform filter
                let layer_item: ImageManifest =
                    match serde_json::from_slice(&self.read_content(namespace, manifest_item).await?) {
                        Ok(s) => s,
                        Err(e) => bail_error!(tid=%tid, error=%e, "JSON error getting ImageManifest"),
                    };
                layer_item.config().to_owned()
            }
            "application/vnd.docker.distribution.manifest.v2+json" => {
                let config_index: ImageManifest = serde_json::from_slice(&content)?;
                debug!(tid=%tid, manifest=?config_index, "config ImageManifest");
                config_index.config().to_owned()
            }
            _ => anyhow::bail!("Don't know how to handle unknown image media type '{}'", media_type),
        };

        // Step 5. load image configuration (layer) from image
        let config: ImageConfiguration =
            match serde_json::from_slice(&self.read_content(namespace, layer_item.digest().to_owned()).await?) {
                Ok(s) => s,
                Err(e) => bail_error!(tid=%tid, error=%e, "JSON error getting ImageConfiguration"),
            };

        debug!(tid=%tid, config=?config, "Loaded ImageConfiguration");

        // Step 6. calculate finalize digest
        let mut iter = config.rootfs().diff_ids().iter();
        let mut prev_digest: String = iter.next().map_or_else(String::new, |v| v.clone());
        while let Some(v) = iter.by_ref().next() {
            let mut hasher = Sha256::new();
            hasher.update(prev_digest);
            hasher.update(" ");
            hasher.update(v);
            let sha = hex::encode(hasher.finalize());
            prev_digest = format!("sha256:{}", sha)
        }
        debug!(tid=%tid, image=%image, digest=%prev_digest, "loaded diff digest");
        Ok(prev_digest)
    }

    /// Ensures that the specified image is available on the machine
    async fn ensure_image(&self, image_name: &String, _tid: &TransactionId) -> Result<()> {
        if self.downloaded_images.contains_key(image_name) {
            return Ok(());
        }
        let output = Command::new("ctr")
            .args([
                "images",
                "pull",
                "--snapshotter",
                self.config.snapshotter.as_str(),
                image_name.as_str(),
            ])
            .output();
        match output {
            Err(e) => anyhow::bail!("Failed to pull the image '{}' because of error {}", image_name, e),
            Ok(output) => {
                if let Some(status) = output.status.code() {
                    if status == 0 {
                        self.downloaded_images.insert(image_name.clone(), true);
                        Ok(())
                    } else {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        anyhow::bail!(
                            "Failed to pull the image '{}' with exit code of '{}', stdout '{}', stderr '{}'",
                            image_name,
                            output.status,
                            stdout,
                            stderr
                        )
                    }
                } else {
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!(
                        "Failed to pull the image '{}' with unkonwn exit code, stdout '{}', stderr '{}'",
                        image_name,
                        stdout,
                        stderr
                    )
                }
            }
        }
    }

    /// Create a container using the given image in the specified namespace
    /// Does not start any process in it
    async fn create_container(
        &self,
        fqdn: &String,
        image_name: &String,
        namespace: &str,
        parallel_invokes: u32,
        mem_limit_mb: MemSizeMb,
        cpus: u32,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
        compute: Compute,
        device_resource: Option<Arc<crate::services::resources::gpu::GPU>>,
    ) -> Result<ContainerdContainer> {
        let port = 8080;

        let permit = match &self.creation_sem {
            Some(sem) => match sem.acquire().await {
                Ok(p) => {
                    debug!(tid=%tid, "Acquired containerd creation semaphore");
                    Some(p)
                }
                Err(e) => {
                    bail_error!(error=%e, tid=%tid, "Error trying to acquire containerd creation semaphore");
                }
            },
            None => None,
        };

        let cid = format!("{}-{}", fqdn, GUID::rand());
        let ns = self.namespace_manager.get_namespace(tid)?;
        debug!(tid=%tid, namespace=%ns.name, containerid=%cid, "Assigning namespace to container");

        let address = &ns.namespace.ips[0].address;

        let spec = self.spec(address, port, mem_limit_mb, cpus, &ns.name, &cid);
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert("owner".to_string(), "iluvatar_worker".to_string());

        let container = Containerd_Container {
            id: cid.to_string(),
            image: image_name.to_string(),
            runtime: Some(Runtime {
                name: "io.containerd.runc.v2".to_string(),
                options: None,
            }),
            spec: Some(spec),
            created_at: None,
            updated_at: None,
            extensions: HashMap::new(),
            labels: labels,
            snapshot_key: "".to_string(),
            snapshotter: self.config.snapshotter.clone(),
        };
        let mut client = ContainersClient::new(self.channel());
        let req = CreateContainerRequest {
            container: Some(container),
        };
        let req = with_namespace!(req, namespace);

        let resp = match client.create(req).await {
            Ok(resp) => resp,
            Err(e) => {
                bail_error!(tid=%tid, error=%e, "Containerd failed to create container");
            }
        };

        debug!(tid=%tid, response=?resp, "Container created");

        let mounts = match self.load_mounts(&cid, &reg.snapshot_base, tid).await {
            Ok(v) => v,
            Err(e) => {
                drop(permit);
                debug!(tid=%tid, "Dropped containerd creation semaphore after load_mounts error");
                return Err(e);
            }
        };
        debug!(tid=%tid, "Mounts loaded");

        let stdin = self.stdin_pth(&cid);
        touch(&stdin)?;
        let stdout = self.stdout_pth(&cid);
        touch(&stdout)?;
        let stderr = self.stderr_pth(&cid);
        touch(&stderr)?;
        let mut client = TasksClient::new(self.channel());

        let req = CreateTaskRequest {
            container_id: cid.clone(),
            rootfs: mounts,
            checkpoint: None,
            options: None,
            stdin: stdin,
            stdout: stdout,
            stderr: stderr,
            terminal: false,
        };
        let req = with_namespace!(req, namespace);
        match client.create(req).await {
            Ok(t) => {
                drop(permit);
                debug!(tid=%tid, "Dropped containerd containerd creation semaphore after success");
                let t = t.into_inner();
                debug!(tid=%tid, task=?t, "Task created");
                let task = Task {
                    pid: t.pid,
                    container_id: Some(cid.clone()),
                    running: false,
                };
                unsafe {
                    Ok(ContainerdContainer::new(
                        cid,
                        task,
                        port,
                        address.clone(),
                        std::num::NonZeroU32::new_unchecked(parallel_invokes),
                        &fqdn,
                        &reg,
                        ns,
                        self.limits_config.timeout_sec,
                        ContainerState::Cold,
                        compute,
                        device_resource,
                    )?)
                }
            }
            Err(e) => {
                drop(permit);
                debug!(tid=%tid, "Dropped containerd containerd creation semaphore after error");
                self.remove_container_internal(&cid, namespace, tid).await?;
                self.namespace_manager.return_namespace(ns, tid)?;
                bail_error!(tid=%tid, error=%e, "Create task failed");
            }
        }
    }

    fn stdout_pth(&self, container_id: &String) -> String {
        temp_file_pth(container_id, "stdout")
    }
    fn stderr_pth(&self, container_id: &String) -> String {
        temp_file_pth(container_id, "stderr")
    }
    fn stdin_pth(&self, container_id: &String) -> String {
        temp_file_pth(container_id, "stdin")
    }
}

#[tonic::async_trait]
impl ContainerIsolationService for ContainerdIsolation {
    fn backend(&self) -> Vec<Isolation> {
        vec![Isolation::CONTAINERD]
    }

    /// creates and starts the entrypoint for a container based on the given image
    /// Run inside the specified namespace
    /// returns a new, unique ID representing it
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, fqdn, image_name, parallel_invokes, namespace, mem_limit_mb, cpus), fields(tid=%tid)))]
    async fn run_container(
        &self,
        fqdn: &String,
        image_name: &String,
        parallel_invokes: u32,
        namespace: &str,
        mem_limit_mb: MemSizeMb,
        cpus: u32,
        reg: &Arc<RegisteredFunction>,
        iso: Isolation,
        compute: Compute,
        device_resource: Option<Arc<crate::services::resources::gpu::GPU>>,
        tid: &TransactionId,
    ) -> Result<Container> {
        if !iso.eq(&Isolation::CONTAINERD) {
            anyhow::bail!("Only supports containerd Isolation, now {:?}", iso);
        }
        info!(tid=%tid, image=%image_name, namespace=%namespace, "Creating container from image");
        let mut container = self
            .create_container(
                fqdn,
                image_name,
                namespace,
                parallel_invokes,
                mem_limit_mb,
                cpus,
                reg,
                tid,
                compute,
                device_resource,
            )
            .await?;
        let mut client = TasksClient::new(self.channel());

        let req = StartRequest {
            container_id: container.container_id.clone(),
            ..Default::default()
        };
        let req = with_namespace!(req, namespace);

        match client.start(req).await {
            Ok(r) => {
                debug!("Task {}: {:?} started", container.container_id, r);
                container.task.running = true;
                Ok(Arc::new(container))
            }
            Err(e) => bail_error!(tid=%tid, error=%e, "Starting task failed"),
        }
    }

    /// Removed the specified container in the containerd namespace
    async fn remove_container(&self, container: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
        let container = crate::services::containers::structs::cast::<ContainerdContainer>(&container, tid)?;
        self.remove_container_internal(&container.container_id, ctd_namespace, tid)
            .await?;
        self.namespace_manager
            .return_namespace(container.namespace.clone(), tid)?;

        info!(tid=%tid, container_id=%container.container_id, "Container deleted");
        Ok(())
    }

    /// Read through an image's digest to find it's snapshot base
    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        _fqdn: &String,
        tid: &TransactionId,
    ) -> Result<()> {
        self.ensure_image(&rf.image_name, tid).await?;
        let snapshot_base = self.search_image_digest(&rf.image_name, "default", tid).await?;
        rf.snapshot_base = snapshot_base;
        Ok(())
    }

    async fn clean_containers(
        &self,
        ctd_namespace: &str,
        self_src: Arc<dyn ContainerIsolationService>,
        tid: &TransactionId,
    ) -> Result<()> {
        info!(tid=%tid, namespace=%ctd_namespace, "Cleaning containers in namespace");
        let mut ctr_client = ContainersClient::new(self.channel());
        let req = ListContainersRequest {
            filters: vec!["labels.\"owner\"==iluvatar_worker".to_string()],
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = match ctr_client.list(req).await {
            Ok(resp) => resp,
            Err(e) => {
                bail_error!(tid=%tid, error=%e, "Containerd failed to list containers");
            }
        };
        debug!(tid=%tid, response=?resp, "Container list response");
        let mut handles = vec![];
        for container in resp.into_inner().containers {
            let container_id = container.id.clone();
            info!(tid=%tid, container_id=%container_id, "Removing container");

            let svc_clone = self_src.clone();
            let ns_clone = ctd_namespace.to_string();
            let tid_clone = tid.to_string();
            handles.push(tokio::spawn(async move {
                let fut = match svc_clone.as_any().downcast_ref::<ContainerdIsolation>() {
                    Some(i) => {
                        futures::future::Either::Left(i.remove_container_internal(&container_id, &ns_clone, &tid_clone))
                    }
                    None => futures::future::Either::Right(async {
                        anyhow::bail!(
                            "Failed to cast ContainerT type {} to {:?}",
                            std::any::type_name::<Arc<dyn ContainerIsolationService>>(),
                            std::any::type_name::<ContainerdIsolation>()
                        )
                    }),
                };
                fut.await
            }));
        }
        let mut failed = 0;
        let num_handles = handles.len();
        for h in handles {
            match h.await {
                Ok(r) => match r {
                    Ok(_r) => (),
                    Err(e) => {
                        error!(tid=%tid, error=%e, "Encountered an error on container cleanup");
                        failed += 1;
                    }
                },
                Err(e) => {
                    error!(tid=%tid, error=%e, "Encountered an error joining thread for container cleanup");
                    failed += 1;
                }
            }
        }
        if failed > 0 {
            anyhow::bail!(
                "There were {} errors encountered cleaning up containers, out of {} containers",
                failed,
                num_handles
            );
        }

        self.namespace_manager
            .clean(self.namespace_manager.clone(), tid)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container, timeout_ms), fields(tid=%tid)))]
    async fn wait_startup(&self, container: &Container, timeout_ms: u64, tid: &TransactionId) -> Result<()> {
        debug!(tid=%tid, container_id=%container.container_id(), "Waiting for startup of container");
        let stderr = self.stderr_pth(&container.container_id());

        let start = SystemTime::now();

        let mut inotify = match Inotify::init() {
            Ok(i) => i,
            Err(e) => bail_error!(error=%e, tid=%tid, "Init inotify watch failed"),
        };
        let dscriptor = match inotify.add_watch(&stderr, WatchMask::MODIFY) {
            Ok(d) => d,
            Err(e) => bail_error!(error=%e, tid=%tid, "Adding inotify watch to file failed"),
        };
        let mut buffer = [0; 256];

        loop {
            match inotify.read_events(&mut buffer) {
                Ok(_events) => {
                    // stderr was written to, gunicorn server is either up or crashed
                    match inotify.rm_watch(dscriptor) {
                        Ok(e) => e,
                        Err(e) => bail_error!(error=%e, tid=%tid, "Deleting inotify watch failed"),
                    };
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed()?.as_millis() as u64 >= timeout_ms {
                        let stdout = self.read_stdout(&container, tid);
                        let stderr = self.read_stderr(&container, tid);
                        if stderr.len() > 0 {
                            warn!(tid=%tid, container_id=%&container.container_id(), "Timeout waiting for container start, but stderr was written to?");
                            return Ok(());
                        }
                        bail_error!(tid=%tid, container_id=%container.container_id(), stdout=%stdout, stderr=%stderr, "Timeout while reading inotify events for container");
                    }
                }
                _ => {
                    bail_error!(tid=%tid, container_id=%container.container_id(), "Error while reading inotify events for container")
                }
            };
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
        let cast_container = match crate::services::containers::structs::cast::<ContainerdContainer>(&container, tid) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=%tid, error=%e, "Error casting container to ContainerdContainer");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
            }
        };
        let contents = match std::fs::read_to_string(format!("/proc/{}/statm", cast_container.task.pid)) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=%tid, error=%e, container_id=%cast_container.container_id, "Error trying to read container /proc/<pid>/statm");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
            }
        };
        let split: Vec<&str> = contents.split(" ").collect();
        // https://linux.die.net/man/5/proc
        // virtual memory resident set size
        let vmrss = split[1];
        container.set_curr_mem_usage(match vmrss.parse::<MemSizeMb>() {
            // multiply page size in bytes by number pages, then convert to mb
            Ok(size_pages) => (size_pages * 4096) / (1024 * 1024),
            Err(e) => {
                warn!(tid=%tid, error=%e, vmrss=%vmrss, "Error trying to parse virtual memory resource set size");
                container.mark_unhealthy();
                container.get_curr_mem_usage()
            }
        });
        container.get_curr_mem_usage()
    }

    fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
        let path = self.stdout_pth(&container.container_id());
        match std::fs::read_to_string(path) {
            Ok(s) => str::replace(&s, "\n", "\\n"),
            Err(e) => {
                error!(tid=%tid, container_id=%container.container_id(), error=%e, "Error reading container");
                format!("STDOUT_READ_ERROR: {}", e)
            }
        }
    }
    fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
        let path = self.stderr_pth(&container.container_id());
        match std::fs::read_to_string(path) {
            Ok(s) => str::replace(&s, "\n", "\\n"),
            Err(e) => {
                error!(tid=%tid, container_id=%container.container_id(), error=%e, "Error reading container");
                format!("STDERR_READ_ERROR: {}", e)
            }
        }
    }
}
impl crate::services::containers::structs::ToAny for ContainerdIsolation {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
