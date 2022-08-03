use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use client::tonic::Code;
use guid_create::GUID;
use crate::services::LifecycleService;
use crate::services::containers::containerd::containerdstructs::{Task, ContainerdContainer};
use crate::transaction::TransactionId;
use crate::types::MemSizeMb;
use crate::utils::{cgroup::cgroup_namespace, port::Port, file::{try_remove_pth, temp_file_pth, touch}};
use crate::bail_error;
use oci_spec::image::{ImageConfiguration, ImageIndex, ImageManifest};
use anyhow::{Result, Context};
use sha2::{Sha256, Digest};
use client::types::Descriptor;
use containerd_client as client;
use containerd_client::tonic::{transport::Channel, Request};
use client::services::v1::{content_client::ContentClient, images_client::ImagesClient};
use client::services::v1::snapshots::{snapshots_client::SnapshotsClient, PrepareSnapshotRequest};
use client::services::v1::{CreateContainerRequest, CreateTaskRequest, StartRequest, DeleteContainerRequest, DeleteTaskRequest, KillRequest, ListContainersRequest};
use client::services::v1::Container as Containerd_Container;
use client::services::v1::{containers_client::ContainersClient, tasks_client::TasksClient};
use client::services::v1::{GetImageRequest, ReadContentRequest};
use client::services::v1::container::Runtime;
use client::with_namespace;
use std::process::Command;
use crate::services::containers::structs::{Container, RegisteredFunction};
use crate::services::network::namespace_manager::NamespaceManager;
use tracing::{info, warn, debug, error}; 
use inotify::{Inotify, WatchMask};

pub mod containerdstructs;

#[derive(Debug)]
pub struct ContainerdLifecycle {
  channel: Option<Channel>,
  namespace_manager: Arc<NamespaceManager>,
}

/// A service to handle the low-level details of containerd container lifecycles:
///   creation, destruction, pulling images, etc
impl ContainerdLifecycle {
  pub fn new(ns_man: Arc<NamespaceManager>) -> ContainerdLifecycle {
    ContainerdLifecycle {
      // this is threadsafe if we clone channel
      // https://docs.rs/tonic/0.4.0/tonic/transport/struct.Channel.html#multiplexing-requests
      channel: None,
      namespace_manager: ns_man
    }
  }

  /// connect to the containerd socket
  pub async fn connect(&mut self) -> Result<()> {
    if let Some(_) = &self.channel {
      Ok(())
    } else {
      let channel = containerd_client::connect("/run/containerd/containerd.sock").await?;
      self.channel = Some(channel);
      Ok(())  
    }
  }

  /// get the channel to the containerd socket
  fn channel(&self) -> Channel {
    self.channel.as_ref().expect("Tried to access channel before opening connection!").clone()
  }

  /// get the default container spec
  fn spec(&self, host_addr: &str, port: Port, mem_limit_mb: MemSizeMb, cpus: u32, net_ns_name: &String, container_id: &String) -> prost_types::Any {
    // https://github.com/opencontainers/runtime-spec/blob/main/config-linux.md
    let spec = include_str!("../../../resources/container_spec.json");
    let spec = spec
        .to_string()
        .replace("$ROOTFS", "rootfs")
        .replace("$OUTPUT", "")
        .replace("$HOST_ADDR", host_addr)
        .replace("$PORT", &port.to_string())
        .replace("$NET_NS", &self.namespace_manager.net_namespace(net_ns_name))
        .replace("\"$MEMLIMIT\"", &(mem_limit_mb*1024*1024).to_string())
//        .replace("\"$SWAPLIMIT\"", &(mem_limit_mb*1024*1024*2).to_string())
        .replace("\"$CPUSHARES\"", &(cpus*1024).to_string())
        .replace("$CGROUPSPATH", &cgroup_namespace(container_id));
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
        Ok(rsp) => {
          match rsp.into_inner().message().await {
            Ok(Some(rsp)) => Ok(rsp.data),
            Ok(None) => bail_error!("Did not get data reading message content {}", ""),
            Err(e) => bail_error!("failed to read content message because {}", e),
          }
        },
        Err(e) => bail_error!("failed to read content because {}", e),
    }
  }
  
  /// get the mount points for a container's (id) snapshot base
  async fn load_mounts(&self, cid: &str, snapshot_base: &String, tid: &TransactionId) -> Result<Vec<containerd_client::types::Mount>> {
    let view_snapshot_req = PrepareSnapshotRequest {
        // TODO: be picky about snapshotter?
        // https://github.com/containerd/containerd/tree/main/docs/snapshotters
        snapshotter: "overlayfs".to_string(),
        key: cid.to_owned(),
        parent: snapshot_base.clone(),
        labels: HashMap::new(),
    };
    let mut cli = SnapshotsClient::new(self.channel());
    let rsp = cli
        .prepare(with_namespace!(view_snapshot_req, "default"))
        .await;
    if let Ok(rsp) = rsp {
      let rsp = rsp.into_inner();
      debug!(tid=%tid, container_id=%cid, mounts=?rsp.mounts, "got mounts");
      Ok(rsp.mounts)
    } else {
      bail_error!("[{}] Failed to prepare snapshot and load mounts: {:?}", tid, rsp);
    }
  }

  async fn delete_task(&self, client: &mut TasksClient<Channel>, container_id: &String, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    let start = SystemTime::now();
    let timeout = Duration::from_secs(10);
    loop {
      match self.try_delete_task(client, container_id, ctd_namespace, tid).await {
        Ok(_) => return Ok(()),
        Err(e2) => {
          if start.elapsed()? > timeout {
            bail_error!("[{}] Deleting task in container '{}' failed with error: {}", tid, container_id, e2);
          }
          // sleep a little and hope the process has terminated in that time
          tokio::time::sleep(Duration::from_millis(5)).await;
        }
      };
    }
  }

  /// Attempts to delete a task
  /// Sometimes this can fail if the internal process hasn't shut down yet (or wasn't killed at all)
  async fn try_delete_task(&self, client: &mut TasksClient<Channel>, container_id: &String, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    let req = DeleteTaskRequest {
      container_id: container_id.clone(),
    };
    let req = with_namespace!(req, ctd_namespace);

    let resp = client
        .delete(req)
        .await;
    match &resp {
      Ok(_) => {
        debug!(tid=%tid, response=?resp, "Delete task response");
        Ok(())
      },
      Err(e) => {
        match e.code() {
          // task crashed and was removed
          Code::NotFound => {
            warn!(tid=%tid, container_id=%container_id, "Task for container was missing when it was attempted to be delete. Usually the process crashed");
            Ok(())
          },
          _ => anyhow::bail!("[{}] Attempt to delete task in container '{}' failed with error: {}", tid, container_id, e),
        }
      },
    }
  }

  async fn kill_task(&self, client: &mut TasksClient<Channel>, container_id: &String, ctd_namespace: &str, tid:&TransactionId) -> Result<()> {
    let req = KillRequest {
      container_id: container_id.clone(),
      // exec_id: container.task.pid.to_string(),
      exec_id: "".to_string(),
      signal: 9, // SIGKILL
      all: true,
    };
    let req = with_namespace!(req, ctd_namespace);

    let resp = client
        .kill(req)
        .await;
    match &resp {
        Ok(_) => {},
        Err(e) => {
          if e.code() == Code::NotFound {
            // task crashed and was removed
            warn!(tid=%tid, container_id=%container_id, "Task for container was missing when it was attempted to be killed");
          } else {
            bail_error!("[{}] Attempt to kill task in container '{}' failed with error: {}", tid, container_id, e);
          }
        },
    };
    debug!(tid=%tid, response=?resp, "Kill task response");
    Ok(())
  }

  async fn delete_containerd_container(&self, client: &mut ContainersClient<Channel>, container_id: &String, ctd_namespace: &str, tid:&TransactionId) -> Result<()> {
    let req = DeleteContainerRequest {
      id: container_id.clone(),
    };
    let req = with_namespace!(req, ctd_namespace);

    let resp = match client
        .delete(req)
        .await {
            Ok(resp) => resp,
            Err(e) => {
              bail_error!("[{}] Delete container failed with error {}", tid, e);
            },
        };
    debug!(tid=%tid, response=?resp, "Delete container response");
    Ok(())
  }

  fn delete_container_resources(&self, container_id: &String, tid: &TransactionId) {
    try_remove_pth(&self.stdin_pth(container_id), tid);
    try_remove_pth(&self.stdout_pth(container_id), tid);
    try_remove_pth(&self.stderr_pth(container_id), tid);
  }

  #[tracing::instrument(skip(self))]
  async fn remove_container_internal(&self, container_id: &String, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    info!("[{}] Removing container '{}'", tid, container_id);
    let mut client = TasksClient::new(self.channel());
    self.kill_task(&mut client, container_id, ctd_namespace, tid).await?;
    self.delete_task(&mut client, container_id, ctd_namespace, tid).await?;

    let mut client = ContainersClient::new(self.channel());
    self.delete_containerd_container(&mut client, container_id, ctd_namespace, tid).await?;
    self.delete_container_resources(container_id, tid);

    info!("[{}] Container: {:?} deleted", tid, container_id);
    Ok(())
  }

  /// Read through an image's digest to find it's snapshot base
  async fn search_image_digest(&self, image: &String, namespace: &str, tid: &TransactionId) -> Result<String> {
    // Step 1. get image digest
    let get_image_req = GetImageRequest { name: image.into() };
    let mut cli = ImagesClient::new(self.channel());
    let rsp = match cli.get(with_namespace!(get_image_req, namespace)).await {
        Ok(rsp) => rsp.into_inner(),
        Err(e) => {
          bail_error!("[{}] Failed to prepare snapshot and load mounts: {:?}", tid, e);
        },
    };
    debug!(tid=%tid,response=?rsp, "image response");
    let (image_digest, media_type) = if let Some(image) = rsp.image {
      image.target
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
            Err(e) => bail_error!("[{}] JSON error getting ImageIndex: {}", tid, e),
        };
        debug!(tid=%tid, index=?config_index, "config ImageIndex");
      
        let manifest_item = config_index
              .manifests()
              .iter()
              .find(|file| match file.platform() {
                  Some(v) => v.architecture().to_string() == "amd64" && v.os().to_string() == "linux",
                  None => false,
              })
              .ok_or_else(|| anyhow::anyhow!("fail to load specific manifest"))?.digest().to_owned();
        
        debug!(tid=%tid, manifest=?manifest_item, "Acquired manifest item");
        // Step 3. load image manifest from specific platform filter
        let layer_item: ImageManifest = match serde_json::from_slice(&self.read_content(namespace, manifest_item).await?) {
            Ok(s) => s,
            Err(e) => bail_error!("[{}] JSON error getting ImageManifest: {}", tid, e),
        };
        layer_item.config().to_owned()
      },
      "application/vnd.docker.distribution.manifest.v2+json" => {
        let config_index: ImageManifest = serde_json::from_slice(&content)?;
        debug!(tid=%tid, manifest=?config_index, "config ImageManifest");
        config_index.config().to_owned()  
      }
      _ => anyhow::bail!("Don't know how to handle unknown image media type '{}'", media_type)
    };

    // Step 5. load image configuration (layer) from image
    let config: ImageConfiguration = match 
        serde_json::from_slice(&self.read_content(namespace, layer_item.digest().to_owned()).await?) {
          Ok(s) => s,
          Err(e) => bail_error!("[{}] JSON error getting ImageConfiguration: {}", tid, e),
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
  async fn ensure_image(&self, image_name: &String) -> Result<()> {
    let output = Command::new("ctr")
          .args(["images", "pull", image_name.as_str()])
          .output();
    match output {
      Err(e) => anyhow::bail!("Failed to pull the image '{}' because of error {}", image_name, e),
      Ok(output) => {
        if let Some(status) = output.status.code() {
          if status == 0 {
            Ok(())
          } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to pull the image '{}' with exit code of '{}', stdout '{}', stderr '{}'", image_name, output.status, stdout, stderr)
          }
        } else {
          let stdout = String::from_utf8_lossy(&output.stdout);
          let stderr = String::from_utf8_lossy(&output.stderr);
          anyhow::bail!("Failed to pull the image '{}' with unkonwn exit code, stdout '{}', stderr '{}'", image_name, stdout, stderr)
        }
      },
    }
  }
  
  /// Create a container using the given image in the specified namespace
  /// Does not start any process in it
  async fn create_container(&self, fqdn: &String, image_name: &String, namespace: &str, parallel_invokes: u32, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<ContainerdContainer> {
    let port = 8080;

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
      snapshotter: "".to_string(),
    };
    let mut client = ContainersClient::new(self.channel());
    let req = CreateContainerRequest {
      container: Some(container),
    };
    let req = with_namespace!(req, namespace);

    let resp =  match client
        .create(req)
        .await {
            Ok(resp) => resp,
            Err(e) => {
              bail_error!("[{}] Containerd failed to create container with error: {}", tid, e);
            },
        };

    debug!(tid=%tid, response=?resp, "Container created");

    let mounts = self.load_mounts(&cid, &reg.snapshot_base, tid).await?;

    let stdin = self.stdin_pth(&cid);
    touch(&stdin)?;
    let stdout = self.stdout_pth(&cid);
    touch(&stdout)?;
    let stderr = self.stderr_pth(&cid);
    touch(&stderr)?;
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
  
    let mut client = TasksClient::new(self.channel());
    match client.create(req).await {
      Ok(t) => {
        let t = t.into_inner();
        debug!(tid=%tid, task=?t, "Task created");
        let task = Task {
          pid: t.pid,
          container_id: Some(cid.clone()),
          running: false
        };
        Ok(ContainerdContainer::new(cid, task, port, address.clone(), parallel_invokes, &fqdn, &reg, ns))
      },
      Err(e) => {
        self.remove_container_internal(&cid, namespace, tid).await?;
        self.namespace_manager.return_namespace(ns, tid)?;
        bail_error!("[{}] Create task failed with: {}", tid, e);
      },
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
impl LifecycleService for ContainerdLifecycle {
  /// creates and starts the entrypoint for a container based on the given image
  /// Run inside the specified namespace
  /// returns a new, unique ID representing it
  #[tracing::instrument(skip(self, reg, fqdn, image_name, parallel_invokes, namespace, mem_limit_mb, cpus))]
  async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    info!("[{}] Creating container from image '{}', in namespace '{}'", tid, image_name, namespace);
    let mut container = self.create_container(fqdn, image_name, namespace, parallel_invokes, mem_limit_mb, cpus, reg, tid).await?;
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
        },
        Err(e) => bail_error!("[{}] Starting task failed with {}", tid, e),
    }
  }

  /// Removed the specified container in the containerd namespace
  async fn remove_container(&self, container: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    let container = crate::services::containers::structs::cast::<ContainerdContainer>(&container, tid)?;
    self.remove_container_internal(&container.container_id, ctd_namespace, tid).await?;
    self.namespace_manager.return_namespace(container.namespace.clone(), tid)?;

    info!("[{}] Container: {:?} deleted", tid, &container.container_id);
    Ok(())
  }

  /// Read through an image's digest to find it's snapshot base
  async fn prepare_function_registration(&self, function_name: &String, function_version: &String, image_name: &String, memory: MemSizeMb, cpus: u32, parallel_invokes: u32, _fqdn: &String, tid: &TransactionId) -> Result<RegisteredFunction> {
    self.ensure_image(&image_name).await?;
    let snapshot_base = self.search_image_digest(&image_name, "default", tid).await?;
    Ok(RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory,
      cpus,
      snapshot_base,
      parallel_invokes,
    })
  }
  
  async fn clean_containers(&self, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    info!("[{}] Cleaning containers in namespace {}", tid, ctd_namespace);
    let mut ctr_client = ContainersClient::new(self.channel());
    let req = ListContainersRequest {
      filters: vec!["labels.\"owner\"==iluvatar_worker".to_string()],
    };
    let req = with_namespace!(req, ctd_namespace);

    let resp =  match ctr_client
        .list(req)
        .await {
            Ok(resp) => resp,
            Err(e) => {
              bail_error!("[{}] Containerd failed to list containers with error: {}", tid, e);
            },
        };
    debug!(tid=%tid, response=?resp, "Container list response");
    for container in resp.into_inner().containers.iter() {
      let container_id = &container.id;
      info!("[{}] Removing container {}", tid, container_id);
      self.remove_container_internal(container_id, ctd_namespace, tid).await?;
    }

    self.namespace_manager.clean(tid)?;
    Ok(())
  }

  #[tracing::instrument(skip(self, container, timeout_ms))]
  async fn wait_startup(&self, container: &Container, timeout_ms: u64, tid: &TransactionId) -> Result<()> {
    debug!(tid=%tid, container_id=%container.container_id(), "Waiting for startup of container");
    let stderr = self.stderr_pth(&container.container_id());

    let start = SystemTime::now();

    let mut inotify = Inotify::init().context("Init inotify watch failed")?;
    let dscriptor = inotify
        .add_watch(&stderr, WatchMask::MODIFY).context("Adding inotify watch failed")?;
    let mut buffer = [0; 256];

    loop {
      match inotify.read_events(&mut buffer) {
        Ok(_events) => {
          // stderr was written to, gunicorn server is either up or crashed
          inotify.rm_watch(dscriptor).context("Deleting inotify watch failed")?;
          break;
        },
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
          if start.elapsed()?.as_millis() as u64 >= timeout_ms {
            let stdout = self.read_stdout(&container, tid);
            let stderr = self.read_stderr(&container, tid);
            if stderr.len() > 0 {
              warn!(tid=%tid, container_id=%&container.container_id(), "Timeout waiting for container start, but stderr was written to?");
              return Ok(())
            }
            bail_error!("[{}] Timeout while reading inotify events for container {}; stdout: '{}'; stderr '{}'", tid, &container.container_id(), stdout, stderr);
          }
        },
        _ => bail_error!("[{}] Error while reading inotify events for container {}", &tid, container.container_id()),
      };
      tokio::time::sleep(std::time::Duration::from_micros(100)).await;
    }
    Ok(())
  }

  #[tracing::instrument(skip(self, container))]
  fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
    let cast_container = match crate::services::containers::structs::cast::<ContainerdContainer>(&container, tid) {
      Ok(c) => c,
      Err(e) => { 
        warn!(tid=%tid, error=%e, "Error casting container to ContainerdContainer");
        return container.get_curr_mem_usage() 
      },
    };
    let contents = match std::fs::read_to_string(format!("/proc/{}/statm", cast_container.task.pid)) {
      Ok(c) => c,
      Err(e) => { 
        warn!(tid=%tid, error=%e, container_id=%cast_container.container_id, "Error trying to read container /proc/<pid>/statm");
        container.mark_unhealthy();
        container.set_curr_mem_usage(cast_container.function.memory);
        return container.get_curr_mem_usage(); 
      },
    };
    let split: Vec<&str> = contents.split(" ").collect();
    // https://linux.die.net/man/5/proc
    // virtual memory resident set size
    let vmrss = split[1];
    container.set_curr_mem_usage(match vmrss.parse::<MemSizeMb>() {
      // multiply page size in bytes by number pages, then convert to mb
      Ok(size_pages) => (size_pages * 4096) / (1024*1024),
      Err(e) => {
        warn!(tid=%tid, error=%e, vmrss=%vmrss, "Error trying to parse virtual memory resource set size");
        cast_container.function.memory
      },
    });
    container.get_curr_mem_usage()
  }

  fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
    let path = self.stdout_pth(&container.container_id());
    match std::fs::read_to_string(path) {
      Ok(s) => str::replace(&s, "\n", "\\n"),
      Err(e) =>  {
        error!("[{}] error reading container '{}' stdout: {}", tid, container.container_id(), e);
        format!("STDOUT_READ_ERROR: {}", e)
      },
    }
  }
  fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
    let path = self.stderr_pth(&container.container_id());
    match std::fs::read_to_string(path) {
      Ok(s) => str::replace(&s, "\n", "\\n"),
      Err(e) =>  {
        error!("[{}] error reading container '{}' stderr: {}", tid, container.container_id(), e);
        format!("STDERR_READ_ERROR: {}", e)
      },
    }
  }
}
