use super::docker::DockerConfig;
use super::ContainerIsolationService;
use crate::services::containers::containerd::containerdstructs::{ContainerdContainer, Task};
use crate::services::containers::structs::{Container, ContainerState};
use crate::services::network::namespace_manager::NamespaceManager;
use crate::services::registration::{RegisteredFunction, RunFunction};
use crate::services::resources::gpu::GPU;
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
// use containerd_client::services::v1::streaming_client::StreamingClient;
// use containerd_client::services::v1::transfer_client::TransferClient;
// use containerd_client::services::v1::TransferRequest;
use containerd_client::tonic::{transport::Channel, Request};
// use containerd_client::types::transfer::{ImageStore, OciRegistry, RegistryResolver, UnpackConfiguration};
// use containerd_client::types::Platform;
use containerd_client::services::v1::GetRequest;
use dashmap::DashMap;
use guid_create::GUID;
use iluvatar_library::clock::now;
use iluvatar_library::threading::tokio_spawn_thread;
use iluvatar_library::types::{err_val, Compute, ContainerServer, Isolation, ResultErrorVal};
use iluvatar_library::utils::file::{container_path, make_paths};
use iluvatar_library::utils::{
    cgroup::cgroup_namespace,
    file::{touch, try_remove_pth},
    port::Port,
    try_get_child_pid,
};
use iluvatar_library::{
    bail_error, bail_error_value, error_value, transaction::TransactionId, types::MemSizeMb, ToAny,
};
use oci_spec::image::{ImageConfiguration, ImageIndex, ImageManifest};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

pub mod containerdstructs;
const CONTAINERD_SOCK: &str = "/run/containerd/containerd.sock";

#[derive(Deserialize)]
pub struct BGPacket {
    pid: u32,
    fqdn: String,
    container_id: String,
    tid: TransactionId,
}

#[allow(dead_code)]
#[derive(ToAny)]
pub struct ContainerdIsolation {
    channel: Option<Channel>,
    namespace_manager: Arc<NamespaceManager>,
    config: Arc<ContainerResourceConfig>,
    limits_config: Arc<FunctionLimits>,
    docker_config: Option<DockerConfig>,
    downloaded_images: Arc<DashMap<String, bool>>,
    creation_sem: Option<tokio::sync::Semaphore>,
    tx: Arc<Sender<BGPacket>>,
    bg_workqueue: JoinHandle<Result<()>>,
}

/// A service to handle the low-level details of containerd container lifecycles:
///   creation, destruction, pulling images, etc
impl ContainerdIsolation {
    pub async fn supported(tid: &TransactionId) -> bool {
        let channel = match containerd_client::connect(CONTAINERD_SOCK).await {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=tid, error=?e, "Failed to connect to containerd socket");
                return false;
            },
        };
        let mut client = VersionClient::new(channel);
        let _: client::tonic::Response<VersionResponse> = match client.version(()).await {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=tid, error=?e, "Failed to query Containerd version");
                return false;
            },
        };
        true
    }

    async fn send_bg_packet(&self, pid: u32, fqdn: &str, container_id: &str, tid: &TransactionId) {
        let _ = self
            .tx
            .send(BGPacket {
                pid,
                fqdn: String::from(fqdn),
                container_id: container_id.to_owned(),
                tid: tid.clone(),
            })
            .await;
    }

    pub fn new(
        ns_man: Arc<NamespaceManager>,
        config: Arc<ContainerResourceConfig>,
        limits_config: Arc<FunctionLimits>,
        docker_config: Option<DockerConfig>,
    ) -> ContainerdIsolation {
        let sem = match config.concurrent_creation {
            0 => None,
            i => Some(tokio::sync::Semaphore::new(i as usize)),
        };

        let (send, mut recv) = tokio::sync::mpsc::channel(30);

        ContainerdIsolation {
            // this is threadsafe if we clone channel
            // https://docs.rs/tonic/0.4.0/tonic/transport/struct.Channel.html#multiplexing-requests
            channel: None,
            namespace_manager: ns_man,
            config,
            limits_config,
            docker_config,
            downloaded_images: Arc::new(DashMap::new()),
            creation_sem: sem,
            tx: Arc::new(send),
            bg_workqueue: tokio_spawn_thread(async move {
                loop {
                    match recv.recv().await {
                        Some(x) => {
                            let ccpid = try_get_child_pid(x.pid, 1, 500).await;
                            info!(
                                      tid=x.tid,
                                      fqdn=%x.fqdn,
                                      container_id=%x.container_id,
                                      pid=%x.pid,
                                      cpid=%ccpid,
                                      "tag_pid_mapping"
                            );
                        },
                        None => {
                            bail_error!("ContainerdIsolation background receive channel broken!");
                        },
                    }
                }
            }),
        }
    }

    /// connect to the containerd socket
    pub async fn connect(&mut self) -> Result<()> {
        if self.channel.is_some() {
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
        reg: &RegisteredFunction,
        net_ns_name: &str,
        container_id: &str,
        entrypoint: Option<Vec<String>>,
        rw_mounts: Option<Vec<(String, String)>>,
    ) -> prost_types::Any {
        // one second of time, in microseconds
        let one_sec_in_us: u64 = 1000 * 1000;
        let mut mounts = vec![];
        let mut env = vec![];
        // https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md
        let mut spec =
            serde_json::from_str::<serde_json::Value>(include_str!("../../../resources/container_spec.json")).unwrap();
        if let Some(entry) = entrypoint {
            spec["process"]["args"] =
                serde_json::Value::Array(entry.into_iter().map(serde_json::Value::String).collect());
        }
        if let Some(rw_mounts) = rw_mounts {
            for (src, dest) in rw_mounts {
                mounts.push(serde_json::json!({
                    "destination": dest,
                    "type": "none",
                    "source": src,
                    "options": [ "rw", "bind" ]
                }));
            }
        }

        spec["root"]["path"] = serde_json::json!("rootfs");
        env.push(serde_json::Value::String(format!("__IL_PORT={}", port)));
        env.push(serde_json::Value::String(format!("__IL_HOST={}", host_addr)));
        env.push(serde_json::Value::String(format!("FLASK_RUN_PORT={}", port)));
        env.push(serde_json::Value::String(format!("FLASK_RUN_HOST={}", host_addr)));
        env.push(serde_json::Value::String(format!(
            "GUNICORN_CMD_ARGS=--workers=1 --timeout={} --bind={}:{} --enable-stdio-inheritance -e PYTHONUNBUFFERED=1",
            self.limits_config.timeout_sec, host_addr, port
        )));

        mounts.push(serde_json::json!({
            "destination": "/etc/resolv.conf",
            "type": "none",
            "source": NamespaceManager::resolv_conf_path(),
            "options": [ "ro", "bind" ]
        }));
        if reg.container_server == ContainerServer::UnixSocket {
            mounts.push(serde_json::json!({
                "source": format!("/tmp/iluvatar/{}/", container_id),
                "destination": "/iluvatar/sockets",
                "options": [ "rw", "bind" ],
                "type": "none"
            }));
            env.push(serde_json::Value::String(format!(
                "__IL_SOCKET={}",
                "/iluvatar/sockets/sock"
            )));
        }
        match &reg.run_info {
            RunFunction::Runtime {
                packages_dir, main_dir, ..
            } => {
                mounts.push(serde_json::json!({
                    "destination": "/packages",
                    "type": "none",
                    "source": packages_dir,
                    "options": [ "ro", "bind" ]
                }));
                mounts.push(serde_json::json!({
                    "destination": "/main",
                    "type": "none",
                    "source": main_dir,
                    "options": [ "ro", "bind" ]
                }));
                env.push(serde_json::Value::String("PYTHONPATH=/main:/packages".to_string()));
            },
            _ => {},
        };

        spec["linux"]["cgroupsPath"] = serde_json::Value::String(cgroup_namespace(container_id));
        spec["linux"]["resources"]["memory"]["limit"] = serde_json::Value::Number((reg.memory * 1024 * 1024).into());
        spec["linux"]["resources"]["cpu"]["quota"] =
            serde_json::Value::Number(((reg.cpus as u64) * one_sec_in_us).into());
        spec["linux"]["resources"]["cpu"]["period"] = serde_json::Value::Number(one_sec_in_us.into());
        spec["linux"]["namespaces"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
              "type": "network",
              "path": NamespaceManager::net_namespace(net_ns_name)
            }));

        spec["mounts"].as_array_mut().unwrap().extend(mounts);
        spec["process"]["env"].as_array_mut().unwrap().extend(env);
        prost_types::Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
            value: spec.to_string().into_bytes(),
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
        snapshot_base: &str,
        tid: &TransactionId,
    ) -> Result<Vec<containerd_client::types::Mount>> {
        let view_snapshot_req = PrepareSnapshotRequest {
            // https://github.com/containerd/containerd/tree/main/docs/snapshotters
            snapshotter: self.config.snapshotter.clone(),
            key: cid.to_owned(),
            parent: snapshot_base.to_owned(),
            labels: HashMap::new(),
        };
        let mut cli = SnapshotsClient::new(self.channel());
        let rsp = cli.prepare(with_namespace!(view_snapshot_req, "default")).await;
        if let Ok(rsp) = rsp {
            let rsp = rsp.into_inner();
            debug!(tid=tid, container_id=%cid, mounts=?rsp.mounts, "got mounts");
            Ok(rsp.mounts)
        } else {
            bail_error!(tid=tid, response=?rsp, "Failed to prepare snapshot and load mounts")
        }
    }

    async fn delete_task(
        &self,
        client: &mut TasksClient<Channel>,
        container_id: &str,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let start = now();
        let timeout = Duration::from_secs(10);
        loop {
            match self.try_delete_task(client, container_id, ctd_namespace, tid).await {
                Ok(_) => return Ok(()),
                Err(e2) => {
                    if start.elapsed() > timeout {
                        bail_error!(tid=tid, container_id=%container_id, error=%e2, "Deleting task in container failed");
                    }
                    // sleep a little and hope the process has terminated in that time
                    tokio::time::sleep(Duration::from_millis(5)).await;
                },
            };
        }
    }

    /// Attempts to delete a task
    /// Sometimes this can fail if the internal process hasn't shut down yet (or wasn't killed at all)
    async fn try_delete_task(
        &self,
        client: &mut TasksClient<Channel>,
        container_id: &str,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let req = DeleteTaskRequest {
            container_id: container_id.to_owned(),
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = client.delete(req).await;
        match &resp {
            Ok(_) => {
                debug!(tid=tid, response=?resp, "Delete task response");
                Ok(())
            },
            Err(e) => {
                match e.code() {
                    // task crashed and was removed
                    Code::NotFound => {
                        warn!(tid=tid, container_id=%container_id, "Task for container was missing when it was attempted to be delete. Usually the process crashed");
                        Ok(())
                    },
                    _ => anyhow::bail!(
                        "[{}] Attempt to delete task in container '{}' failed with error: {}",
                        tid,
                        container_id,
                        e
                    ),
                }
            },
        }
    }

    async fn kill_task(
        &self,
        client: &mut TasksClient<Channel>,
        container_id: &str,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let req = KillRequest {
            container_id: container_id.to_owned(),
            // exec_id: container.task.pid.to_string(),
            exec_id: "".to_string(),
            signal: 9, // SIGKILL
            all: true,
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = client.kill(req).await;
        match &resp {
            Ok(_) => {},
            Err(e) => {
                if e.code() == Code::NotFound {
                    // task crashed and was removed
                    warn!(tid=tid, container_id=%container_id, "Task for container was missing when it was attempted to be killed");
                } else {
                    bail_error!(tid=tid, container_id=%container_id, error=%e, "Attempt to kill task in container failed");
                }
            },
        };
        debug!(tid=tid, response=?resp, "Kill task response");
        Ok(())
    }

    async fn delete_containerd_container(
        &self,
        client: &mut ContainersClient<Channel>,
        container_id: &str,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        let req = DeleteContainerRequest {
            id: container_id.to_owned(),
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = match client.delete(req).await {
            Ok(resp) => resp,
            Err(e) => bail_error!(tid=tid, error=%e, "Delete container failed"),
        };
        debug!(tid=tid, response=?resp, "Delete container response");
        Ok(())
    }

    fn delete_container_resources(&self, container_id: &str, tid: &TransactionId) {
        try_remove_pth(container_path(container_id), tid)
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=tid)))]
    async fn remove_container_internal(
        &self,
        container_id: &str,
        ctd_namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        info!(tid=tid, container_id=%container_id, "Removing container");
        let mut client = TasksClient::new(self.channel());
        self.kill_task(&mut client, container_id, ctd_namespace, tid).await?;
        self.delete_task(&mut client, container_id, ctd_namespace, tid).await?;

        let mut client = ContainersClient::new(self.channel());
        self.delete_containerd_container(&mut client, container_id, ctd_namespace, tid)
            .await?;
        self.delete_container_resources(container_id, tid);

        debug!(tid=tid, container_id=%container_id, "Container deleted");
        Ok(())
    }

    /// Read through an image's digest to find its snapshot base
    async fn search_image_digest(&self, image: &str, namespace: &str, tid: &TransactionId) -> Result<String> {
        // Step 1. get image digest
        let get_image_req = GetImageRequest { name: image.into() };
        let mut cli = ImagesClient::new(self.channel());
        let rsp = match cli.get(with_namespace!(get_image_req, namespace)).await {
            Ok(rsp) => rsp.into_inner(),
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to get image"),
        };
        debug!(tid = tid, "image response");
        let (image_digest, media_type) = if let Some(image) = rsp.image {
            image
                .target
                .ok_or_else(|| anyhow::anyhow!("Could not find image digest"))
                .map(|v: Descriptor| (v.digest, v.media_type))?
        } else {
            anyhow::bail!("Could not find image")
        };

        debug!(tid = tid, "got image digest");

        // Step 2. get image content manifests
        let content = self.read_content(namespace, image_digest).await?;

        let layer_item = match media_type.as_str() {
            "application/vnd.docker.distribution.manifest.list.v2+json" => {
                let config_index: ImageIndex = match serde_json::from_slice(&content) {
                    Ok(s) => s,
                    Err(e) => bail_error!(tid=tid, error=%e, "JSON error getting ImageIndex"),
                };
                debug!(tid = tid, "config ImageIndex");

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

                debug!(tid = tid, "Acquired manifest item");
                // Step 3. load image manifest from specific platform filter
                let layer_item: ImageManifest =
                    match serde_json::from_slice(&self.read_content(namespace, manifest_item.to_string()).await?) {
                        Ok(s) => s,
                        Err(e) => bail_error!(tid=tid, error=%e, "JSON error getting ImageManifest"),
                    };
                layer_item.config().to_owned()
            },
            "application/vnd.docker.distribution.manifest.v2+json" => {
                let config_index: ImageManifest = serde_json::from_slice(&content)?;
                debug!(tid = tid, "config ImageManifest");
                config_index.config().to_owned()
            },
            _ => anyhow::bail!("Don't know how to handle unknown image media type '{}'", media_type),
        };

        // Step 5. load image configuration (layer) from image
        let config: ImageConfiguration =
            match serde_json::from_slice(&self.read_content(namespace, layer_item.digest().to_string()).await?) {
                Ok(s) => s,
                Err(e) => bail_error!(tid=tid, error=%e, "JSON error getting ImageConfiguration"),
            };

        debug!(tid=tid, config=?config, "Loaded ImageConfiguration");

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
        debug!(tid = tid, "loaded diff digest");
        Ok(prev_digest)
    }

    /// Ensures that the specified image is available on the machine
    async fn ensure_image(&self, image_name: &str, tid: &TransactionId, _namespace: &str) -> Result<()> {
        if self.downloaded_images.contains_key(image_name) {
            return Ok(());
        }

        if self.downloaded_images.contains_key(image_name) {
            return Ok(());
        }
        let mut args = vec!["images", "pull", "--snapshotter", self.config.snapshotter.as_str()];
        let auth_str;
        if let Some(docker) = &self.docker_config {
            if let Some(auth) = &docker.auth {
                if !auth.repository.is_empty() && image_name.starts_with(auth.repository.as_str()) {
                    args.push("--user");
                    auth_str = format!("{}:{}", auth.username, auth.password);
                    args.push(auth_str.as_str());
                }
            }
        }
        args.push(image_name);
        let output = iluvatar_library::utils::execute_cmd_async("/usr/bin/ctr", args, None, tid);
        match output.await {
            Err(e) => anyhow::bail!("Failed to pull the image '{}' because of error {}", image_name, e),
            Ok(output) => {
                if let Some(status) = output.status.code() {
                    if status == 0 {
                        self.downloaded_images.insert(image_name.to_owned(), true);
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
            },
        }
        // let mut resolver = None;
        // let mut _stream_client;
        // let mut _stream = None;
        // let trans_options = None;
        // let stream_uuid = GUID::rand().to_string();
        // if let Some(docker) = &self.docker_config {
        //     if let Some(auth) = &docker.auth {
        //         if !auth.repository.is_empty() && image_name.starts_with(auth.repository.as_str()) {
        //             _stream_client = StreamingClient::new(self.channel().clone());
        //             let req = containerd_client::services::v1::StreamInit {
        //                 id: stream_uuid.clone(),
        //             };
        //             let req = containerd_client::to_any(&req);
        //             // let req= with_namespace!(req, namespace);
        //             // let req = containerd_client::to_any(&req);
        //             // tokio_stream::iter(any_init)
        //             info!(tid=tid, uuid=%stream_uuid, "sending stream request");
        //
        //             let mut stream = match _stream_client.stream(tokio_stream::iter([req])).await {
        //                 Ok(s) => s.into_inner(),
        //                 Err(e) => bail_error!(tid=tid, error=%e, "stream init failed"),
        //             };
        //             info!(tid=tid, "checking stream 1!");
        //             match stream.message().await {
        //                 Err(e) => bail_error!(tid=tid, error=%e, "rcv stream init failed"),
        //                 Ok(None) => info!(tid=tid, "init stream closed?"),
        //                 Ok(Some(val)) => info!(tid=tid, value=?val, "init stream value"),
        //             };
        //             _stream = Some(stream);
        //             // trans_options = Some(containerd_client::services::v1::TransferOptions { progress_stream:stream_uuid.clone() });
        //             resolver = Some(RegistryResolver {
        //                 auth_stream: stream_uuid.clone(),
        //                 ..Default::default()
        //             });
        //         }
        //     }
        // }
        // tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        // let source = OciRegistry {
        //     reference: image_name.to_string(),
        //     resolver,
        // };
        // let arch = match std::env::consts::ARCH {
        //     "x86_64" => "amd64",
        //     "aarch64" => "arm64",
        //     _ => std::env::consts::ARCH,
        // };
        // let platform = Platform {
        //     os: "linux".to_string(),
        //     architecture: arch.to_string(),
        //     ..Default::default()
        // };
        //
        // let destination = ImageStore {
        //     name: image_name.to_string(),
        //     platforms: vec![platform.clone()],
        //     unpacks: vec![UnpackConfiguration {
        //         platform: Some(platform),
        //         snapshotter: self.config.snapshotter.to_owned(),
        //     }],
        //     ..Default::default()
        // };
        //
        // let anys = containerd_client::to_any(&source);
        // let anyd = containerd_client::to_any(&destination);
        // let request = TransferRequest {
        //     source: Some(anys),
        //     destination: Some(anyd),
        //     options: trans_options,
        // };
        // // Execute the transfer (pull)
        // info!(tid=tid, "pulling image");
        // // if let Some(stream) = _stream.as_mut() {
        // //     info!(tid=tid, "checking stream 2!");
        // //     match stream.message().await {
        // //         Err(e) => bail_error!(tid=tid, error=%e, "rcv stream init failed"),
        // //         Ok(None) => info!(tid=tid, "init stream closed?"),
        // //         Ok(Some(val)) => info!(tid=tid, value=?val, "init stream value")
        // //     };
        // // }
        // let cnl = self.channel().clone();
        // let nm = namespace.to_string();
        // let j = tokio_spawn_thread(async move {
        //     let mut client = TransferClient::new(cnl);
        //     client.transfer(with_namespace!(request, nm)).await
        // });
        // // let t = .await;
        // // if let Some(stream) = _stream.as_mut() {
        // //     info!(tid=tid, "checking stream 3!");
        // //     match stream.message().await {
        // //         Err(e) => bail_error!(tid=tid, error=%e, "rcv stream init failed"),
        // //         Ok(None) => info!(tid=tid, "init stream closed?"),
        // //         Ok(Some(val)) => info!(tid=tid, value=?val, "init stream value")
        // //     };
        // // }
        // match j.await? {
        //     Ok(_) => Ok(()),
        //     Err(e) => bail_error!(tid=tid, error=%e, image_name=image_name, "Error pulling image"),
        // }
    }

    /// Create a container using the given image in the specified namespace
    /// Does not start any process in it
    async fn create_container(
        &self,
        namespace: &str,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
        compute: Compute,
        device_resource: Option<GPU>,
        entrypoint: Option<Vec<String>>,
        rw_mounts: Option<Vec<(String, String)>>,
    ) -> ResultErrorVal<ContainerdContainer, Option<GPU>> {
        let port = 8080;

        let permit = match &self.creation_sem {
            Some(sem) => match sem.acquire().await {
                Ok(p) => {
                    debug!(tid = tid, "Acquired containerd creation semaphore");
                    Some(p)
                },
                Err(e) => {
                    bail_error_value!(error=%e, "Error trying to acquire containerd creation semaphore", device_resource);
                },
            },
            None => None,
        };

        let cid = format!("{}-{}", reg.fqdn, GUID::rand());
        let ns = match self.namespace_manager.get_namespace(tid) {
            Ok(n) => n,
            Err(e) => return err_val(e, device_resource),
        };
        debug!(tid=tid, namespace=%ns.name, containerid=%cid, "Assigning namespace to container");

        let address = &ns.namespace.ips[0].address;

        let spec = self.spec(address, port, reg, &ns.name, &cid, entrypoint, rw_mounts);
        let mut labels: HashMap<String, String> = HashMap::new();
        labels.insert("owner".to_string(), "iluvatar_worker".to_string());

        let container = Containerd_Container {
            id: cid.to_string(),
            image: reg.image_name.to_string(),
            runtime: Some(Runtime {
                name: "io.containerd.runc.v2".to_string(),
                options: None,
            }),
            spec: Some(spec),
            created_at: None,
            updated_at: None,
            sandbox: "".to_owned(),
            extensions: HashMap::new(),
            labels,
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
                bail_error_value!(tid=tid, error=%e, "Containerd failed to create container", device_resource);
            },
        };

        debug!(tid=tid, response=?resp, "Container created");

        let mounts = match self.load_mounts(&cid, &reg.snapshot_base, tid).await {
            Ok(v) => v,
            Err(e) => {
                drop(permit);
                debug!(
                    tid = tid,
                    "Dropped containerd creation semaphore after load_mounts error"
                );
                return Err((e, device_resource));
            },
        };
        debug!(tid = tid, "Mounts loaded");
        let resources_dir = container_path(&cid);
        if let Err(e) = make_paths(&resources_dir, tid) {
            bail_error_value!(tid=tid, error=%e, "make_paths failed", device_resource);
        };

        let stdin = self.stdin_pth(&cid);
        if let Err(e) = touch(&stdin) {
            return err_val(anyhow::Error::new(e), device_resource);
        };
        let stdout = self.stdout_pth(&cid);
        if let Err(e) = touch(&stdout) {
            return err_val(anyhow::Error::new(e), device_resource);
        };
        let stderr = self.stderr_pth(&cid);
        if let Err(e) = touch(&stderr) {
            return err_val(anyhow::Error::new(e), device_resource);
        };
        let mut client = TasksClient::new(self.channel());

        let req = CreateTaskRequest {
            container_id: cid.clone(),
            rootfs: mounts,
            checkpoint: None,
            options: None,
            stdin: stdin.to_string_lossy().to_string(),
            stdout: stdout.to_string_lossy().to_string(),
            stderr: stderr.to_string_lossy().to_string(),
            terminal: false,
            runtime_path: "".to_owned(),
        };
        // match std::os::unix::net::UnixListener::bind("/tmp/iluvatar/socks/ctr") {
        //     Ok(_) => info!(tid=tid, "socket created OK"),
        //     Err(e) => error!(tid=tid, error=%e, "socket creation error"),
        // };
        let req = with_namespace!(req, namespace);
        match client.create(req).await {
            Ok(t) => {
                drop(permit);
                debug!(tid = tid, "Dropped containerd creation semaphore after success");
                let t = t.into_inner();
                debug!(tid=tid, task=?t, "Task created");
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
                        std::num::NonZeroU32::new_unchecked(reg.parallel_invokes),
                        reg,
                        ns,
                        self.limits_config.timeout_sec,
                        ContainerState::Cold,
                        compute,
                        device_resource,
                        tid,
                    )
                    .await?)
                }
            },
            Err(e) => {
                drop(permit);
                debug!(
                    tid = tid,
                    "Dropped containerd containerd creation semaphore after error"
                );
                if let Err(e) = self.remove_container_internal(&cid, namespace, tid).await {
                    return err_val(e, device_resource);
                };
                if let Err(e) = self.namespace_manager.return_namespace(ns, tid) {
                    return err_val(e, device_resource);
                };
                bail_error_value!(tid=tid, error=%e, "Create task failed", device_resource);
            },
        }
    }

    /// Install package dependencies of function that uploaded code.
    async fn dependencies(&self, rf: &RegisteredFunction, tid: &TransactionId) -> Result<()> {
        let pkg_namespace = "package";
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
            "/install/packages",
            "-r",
            "/install/main/reqs.txt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let mut mounts = vec![];
        match &rf.run_info {
            RunFunction::Runtime { packages_dir, main_dir } => {
                make_paths(Path::new(packages_dir), tid)?;
                make_paths(Path::new(main_dir), tid)?;
                mounts.push((packages_dir.clone(), "/install/packages".to_string()));
                mounts.push((main_dir.clone(), "/install/main".to_string()));
            },
            // this func should only be called if using a runtime
            _ => unreachable!(),
        };
        info!(tid = tid, "starting package install");
        match self
            .create_container(
                pkg_namespace,
                &Arc::new(rf.clone()),
                tid,
                Compute::CPU,
                None,
                Some(entrypoint),
                Some(mounts),
            )
            .await
        {
            Ok(ctr) => {
                let mut client = TasksClient::new(self.channel());
                let req = StartRequest {
                    container_id: ctr.container_id.clone(),
                    ..Default::default()
                };
                match client.start(with_namespace!(req, pkg_namespace)).await {
                    Ok(_) => (),
                    Err(e) => {
                        bail_error!(tid=tid, error=%e, "Starting task failed");
                    },
                };

                debug!(tid = tid, "waiting for package install to finish");
                loop {
                    let r = GetRequest {
                        container_id: ctr.container_id.clone(),
                        ..Default::default()
                    };
                    let task = client.get(with_namespace!(r, pkg_namespace)).await?.into_inner();
                    info!(tid = tid, task=?task, "task deets");
                    if let Some(proc) = task.process {
                        match proc.status() {
                            containerd_client::types::v1::Status::Stopped => {
                                let ctr: Container = Arc::new(ctr);
                                if proc.exit_status == 0 {
                                    debug!(tid = tid, "package install finished");
                                    return self.remove_container(ctr, pkg_namespace, tid).await;
                                } else {
                                    let stdout = self.read_stdout(&ctr, tid).await;
                                    let stderr = self.read_stderr(&ctr, tid).await;
                                    if let Err(e) = self.remove_container(ctr, pkg_namespace, tid).await {
                                        error!(tid=tid, error=%e, "cleanup removal failed");
                                    };
                                    bail_error!(
                                        tid = tid,
                                        exit_status = proc.exit_status,
                                        stdout = stdout,
                                        stderr = stderr,
                                        "bad exit on packages"
                                    );
                                }
                            },
                            _ => (),
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            },
            Err(e) => Err(e.0),
        }
    }

    fn stdout_pth(&self, container_id: &str) -> PathBuf {
        container_path(container_id).join("stdout")
    }
    fn stderr_pth(&self, container_id: &str) -> PathBuf {
        container_path(container_id).join("stderr")
    }
    fn stdin_pth(&self, container_id: &str) -> PathBuf {
        container_path(container_id).join("stdin")
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
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, namespace, iso, compute, device_resource), fields(tid=tid)))]
    async fn run_container(
        &self,
        namespace: &str,
        reg: &Arc<RegisteredFunction>,
        iso: Isolation,
        compute: Compute,
        device_resource: Option<GPU>,
        tid: &TransactionId,
    ) -> ResultErrorVal<Container, Option<GPU>> {
        if !iso.eq(&Isolation::CONTAINERD) {
            error_value!("Only supports containerd Isolation, now {:?}", iso, device_resource);
        }
        info!(tid=tid, namespace=%namespace, "Creating container from image");
        let mut container = self
            .create_container(namespace, reg, tid, compute, device_resource, None, None)
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
                self.send_bg_packet(
                    container.task.pid,
                    &reg.fqdn,
                    &container.task.container_id.clone().unwrap(),
                    tid,
                )
                .await;
                Ok(Arc::new(container))
            },
            Err(e) => {
                bail_error_value!(tid=tid, error=%e, "Starting task failed", crate::services::containers::structs::ContainerT::revoke_device(&container));
            },
        }
    }

    /// Removed the specified container in the containerd namespace
    async fn remove_container(&self, container: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
        let container = crate::services::containers::structs::cast::<ContainerdContainer>(&container)?;
        self.remove_container_internal(&container.container_id, ctd_namespace, tid)
            .await?;
        self.namespace_manager
            .return_namespace(container.namespace.clone(), tid)?;

        info!(tid=tid, container_id=%container.container_id, "Container deleted");
        Ok(())
    }

    /// Read through an image's digest to find it's snapshot base
    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        namespace: &str,
        tid: &TransactionId,
    ) -> Result<()> {
        self.ensure_image(&rf.image_name, tid, namespace).await?;
        let snapshot_base = self.search_image_digest(&rf.image_name, "default", tid).await?;
        rf.snapshot_base = snapshot_base;
        match &rf.run_info {
            RunFunction::Runtime { .. } => self.dependencies(rf, tid).await,
            _ => Ok(()),
        }
    }

    async fn clean_containers(
        &self,
        ctd_namespace: &str,
        self_src: Arc<dyn ContainerIsolationService>,
        tid: &TransactionId,
    ) -> Result<()> {
        info!(tid=tid, namespace=%ctd_namespace, "Cleaning containers in namespace");
        let mut ctr_client = ContainersClient::new(self.channel());
        let req = ListContainersRequest {
            filters: vec!["labels.\"owner\"==iluvatar_worker".to_string()],
        };
        let req = with_namespace!(req, ctd_namespace);

        let resp = match ctr_client.list(req).await {
            Ok(resp) => resp,
            Err(e) => {
                bail_error!(tid=tid, error=%e, "Containerd failed to list containers");
            },
        };
        debug!(tid=tid, response=?resp, "Container list response");
        let mut handles = vec![];
        for container in resp.into_inner().containers {
            let container_id = container.id.clone();
            info!(tid=tid, container_id=%container_id, "Removing container");

            let svc_clone = self_src.clone();
            let ns_clone = ctd_namespace.to_string();
            let tid_clone = tid.to_string();
            handles.push(tokio_spawn_thread(async move {
                let fut = match svc_clone.as_any().downcast_ref::<ContainerdIsolation>() {
                    Some(i) => {
                        futures::future::Either::Left(i.remove_container_internal(&container_id, &ns_clone, &tid_clone))
                    },
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
                        error!(tid=tid, error=%e, "Encountered an error on container cleanup");
                        failed += 1;
                    },
                },
                Err(e) => {
                    error!(tid=tid, error=%e, "Encountered an error joining thread for container cleanup");
                    failed += 1;
                },
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

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container, timeout_ms), fields(tid=tid)))]
    async fn wait_startup(&self, container: &Container, timeout_ms: u64, tid: &TransactionId) -> Result<()> {
        info!(
            tid = tid,
            container_id = container.container_id(),
            "Waiting for startup of container"
        );
        let start = now();
        let stderr_pth = self.stderr_pth(container.container_id());

        loop {
            if let Ok(c) = tokio::fs::try_exists(&stderr_pth).await {
                if !c {
                    bail_error!(
                        tid = tid,
                        container_id = container.container_id(),
                        "Broken file waiting on container startup"
                    );
                } else {
                    let stderr = self.read_stderr(container, tid).await;
                    // stderr will have container startup magic string
                    if stderr.contains("MGK_GUN_READY_KMG") {
                        info!(
                            tid = tid,
                            container_id = container.container_id(),
                            "container successfully started!"
                        );
                        return Ok(());
                    }
                }
            }
            if start.elapsed() >= Duration::from_secs(timeout_ms) {
                let stdout = self.read_stdout(container, tid).await;
                let stderr = self.read_stderr(container, tid).await;
                bail_error!(
                    tid = tid,
                    container_id = container.container_id(),
                    stdout = stdout,
                    stderr = stderr,
                    "Timeout while waiting container startup"
                );
            }
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=tid)))]
    async fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
        let cast_container = match crate::services::containers::structs::cast::<ContainerdContainer>(container) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=tid, error=%e, "Error casting container to ContainerdContainer");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
            },
        };
        let contents = match std::fs::read_to_string(format!("/proc/{}/statm", cast_container.task.pid)) {
            Ok(c) => c,
            Err(e) => {
                warn!(tid=tid, error=%e, container_id=cast_container.container_id, "Error trying to read container /proc/<pid>/statm");
                container.mark_unhealthy();
                return container.get_curr_mem_usage();
            },
        };
        let split: Vec<&str> = contents.split(' ').collect();
        // https://linux.die.net/man/5/proc
        // virtual memory resident set size
        let vmrss = split[1];
        container.set_curr_mem_usage(match vmrss.parse::<MemSizeMb>() {
            // multiply page size in bytes by number pages, then convert to mb
            Ok(size_pages) => (size_pages * 4096) / (1024 * 1024),
            Err(e) => {
                warn!(tid=tid, error=%e, vmrss=vmrss, "Error trying to parse virtual memory resource set size");
                container.mark_unhealthy();
                container.get_curr_mem_usage()
            },
        });
        container.get_curr_mem_usage()
    }

    async fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
        let path = self.stdout_pth(container.container_id());
        match std::fs::read_to_string(path) {
            Ok(s) => str::replace(&s, "\n", "\\n"),
            Err(e) => {
                error!(tid=tid, container_id=container.container_id(), error=%e, "Error reading container stdout");
                format!("STDOUT_READ_ERROR: {}", e)
            },
        }
    }
    async fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
        let path = self.stderr_pth(container.container_id());
        match std::fs::read_to_string(path) {
            Ok(s) => str::replace(&s, "\n", "\\n"),
            Err(e) => {
                error!(tid=tid, container_id=container.container_id(), error=%e, "Error reading container stderr");
                format!("STDERR_READ_ERROR: {}", e)
            },
        }
    }
}
