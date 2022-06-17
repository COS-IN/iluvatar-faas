use std::collections::HashMap;
use std::sync::Arc;

use client::services::v1::containers_client::ContainersClient;
use client::services::v1::tasks_client::TasksClient;
use guid_create::GUID;
use iluvatar_lib::utils::{Port, calculate_base_uri, calculate_invoke_uri};
use log::*;
use oci_spec::image::{ImageConfiguration, ImageIndex, ImageManifest};
use anyhow::Result;
use sha2::{Sha256, Digest};
use client::types::Descriptor;
use containerd_client as client;
use containerd_client::tonic::transport::Channel;
use client::services::v1::content_client::ContentClient;
use client::services::v1::images_client::ImagesClient;
use client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use client::services::v1::snapshots::PrepareSnapshotRequest;
use client::services::v1::{CreateContainerRequest, CreateTaskRequest, StartRequest, DeleteContainerRequest};
use client::services::v1::Container as Containerd_Container;
use client::services::v1::{GetImageRequest, ReadContentRequest};
use client::services::v1::container::Runtime;
use client::with_namespace;
use containerd_client::tonic::Request;
use prost_types::Any;
use std::process::Command;
use crate::containers::structs::{Container, Task};
use crate::network::namespace_manager::NamespaceManager;

pub struct ContainerLifecycle {
  channel: Option<Channel>,
  namespace_manager: Arc<NamespaceManager>,
}

/// A service to handle the low-level details of container lifecycles:
///   creation, destruction, pulling images, etc
/// NOT THREAD SAFE
///   TODO: is this safe to share the channel?
impl ContainerLifecycle {
  pub fn new(ns_man: Arc<NamespaceManager>) -> ContainerLifecycle {
    ContainerLifecycle {
      channel: None,
      namespace_manager: ns_man
    }
  }

  /// connect to the containerd socket
  async fn connect(&mut self) -> Result<()> {
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
  fn spec(&self, host_addr: &str, port: Port, container_id: &String, mem_limit_mb: u32) -> Any {
    let spec = include_str!("../resources/container_spec.json");
    let spec = spec
        .to_string()
        .replace("$ROOTFS", "rootfs")
        .replace("$OUTPUT", "")
        .replace("$HOST_ADDR", host_addr)
        .replace("$PORT", &port.to_string())
        .replace("$NET_NS", &self.namespace_manager.net_namespace(container_id))
        .replace("\"$MEMLIMIT\"", &(mem_limit_mb*1024*1024).to_string());
    
    Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: spec.into_bytes(),
    }
  }

  async fn read_content(&mut self, namespace: &str, digest: String) -> Result<Vec<u8>> {
    let read_content_req = ReadContentRequest {
        digest,
        offset: 0,
        size: 0,
    };
    self.connect().await?;
    let mut cli = ContentClient::new(self.channel());
  
    let mut rsp = cli.read(with_namespace!(read_content_req, namespace)).await?.into_inner();
    if let Some(rsp) = rsp.message().await? {
      return Ok(rsp.data);
    }
    anyhow::bail!("failed to read content")
  }
  
  /// Read through an image's digest to find it's snapshot base
  pub async fn search_image_digest(&mut self, image: &String, namespace: &str) -> Result<String> {
    self.connect().await?;
    // Step 1. get image digest
    let get_image_req = GetImageRequest { name: image.into() };
    let mut cli = ImagesClient::new(self.channel());
    let rsp = cli.get(with_namespace!(get_image_req, namespace)).await?.into_inner();
    debug!("image resp = {:?}", rsp);
    let (image_digest, media_type) = if let Some(image) = rsp.image {
      image.target
            .ok_or_else(|| anyhow::anyhow!("Could not find image digest"))
          .map(|v: Descriptor| (v.digest, v.media_type))?
    } else {
      anyhow::bail!("Could not find image")
    };
  
    debug!("got image {} info {:?}", image, image_digest);
  
    // Step 2. get image content manifests
    let content = self.read_content(namespace, image_digest).await?;
    let layer_item = match media_type.as_str() {
      "application/vnd.docker.distribution.manifest.list.v2+json" => {
        let config_index: ImageIndex = serde_json::from_slice(&content)?;
        debug!("config ImageIndex = {:?}", config_index);
      
        let manifest_item = config_index
              .manifests()
              .iter()
              .find(|file| match file.platform() {
                  Some(v) => v.architecture().to_string() == "amd64" && v.os().to_string() == "linux",
                  None => false,
              })
              .ok_or_else(|| anyhow::anyhow!("fail to load specific manifest"))?.digest().to_owned();
        
        debug!("Acquired manifest item: {}", manifest_item);
        // Step 3. load image manifest from specific platform filter
        let layer_item: ImageManifest =
          serde_json::from_slice(&self.read_content(namespace, manifest_item).await?)?;
        layer_item.config().to_owned()
     },
      "application/vnd.docker.distribution.manifest.v2+json" => {
        let config_index: ImageManifest = serde_json::from_slice(&content)?;
        debug!("config ImageManifest = {:?}", config_index);
        config_index.config().to_owned()  
      }
      _ => anyhow::bail!("Don't know how to handle unknown image media type '{}'", media_type)
    };

    // Step 5. load image configuration (layer) from image
    let config: ImageConfiguration =
        serde_json::from_slice(&self.read_content(namespace, layer_item.digest().to_owned()).await?)?;
  
    debug!("Loaded ImageConfiguration: {:?}", config);

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
    debug!("load {} diff digest {}", image, prev_digest);
    Ok(prev_digest)
  }
  
  /// get the mount points for a container's (id) snapshot base
  async fn load_mounts(&mut self, id: &str, snapshot_base: String) -> Result<Vec<containerd_client::types::Mount>> {
    let view_snapshot_req = PrepareSnapshotRequest {
        // TODO: be picky about snapshotter?
        // https://github.com/containerd/containerd/tree/main/docs/snapshotters
        snapshotter: "overlayfs".to_string(),
        key: id.to_owned(),
        parent: snapshot_base,
        labels: HashMap::new(),
    };
    self.connect().await?;
    let mut cli = SnapshotsClient::new(self.channel());
    let rsp = cli
        .prepare(with_namespace!(view_snapshot_req, "default"))
        .await?
        .into_inner();
  
    debug!("got mounts {} {}", id, rsp.mounts.len());
    Ok(rsp.mounts)
  }

  /// Create a container using the given image in the specified namespace
  /// Does not start any process in it
  pub async fn create_container(&mut self, image_name: &String, namespace: &str, parallel_invokes: u32, mem_limit_mb: u32) -> Result<Container> {
    let port = 8080;

    let cid = GUID::rand().to_string();
    let ns = self.namespace_manager.create_namespace(&cid)?;

    let address = &ns.ips[0].address;

    let spec = self.spec(address, port, &cid, mem_limit_mb);

    let uri = calculate_invoke_uri(address, port);
    let base_uri = calculate_base_uri(address, port);

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
      labels: HashMap::new(),
      snapshot_key: "".to_string(),
      snapshotter: "".to_string(),
    };
    self.connect().await?;
    let mut client = ContainersClient::new(self.channel());
    let req = CreateContainerRequest {
      container: Some(container),
    };
    let req = with_namespace!(req, namespace);

    let resp = client
        .create(req)
        .await?;

    debug!("Container: created {:?}", resp);

    let snapshot_base = self.search_image_digest(image_name, "default").await?;

    let mounts = self.load_mounts(&cid, snapshot_base).await.unwrap();

    let req = CreateTaskRequest {
        container_id: cid.clone(),
        rootfs: mounts,
        checkpoint: None,
        options: None,
        stdin: "".into(),
        stdout: "".into(),
        stderr: "".into(),
        terminal: false,
    };
    let req = with_namespace!(req, namespace);
  
    let mut client = TasksClient::new(self.channel());
    let resp = client.create(req).await?;
    debug!("Task: created {:?}", resp);

    Ok(Container {
      container_id: cid,
      address: address.to_owned(),
      port: port,
      invoke_uri: uri,
      base_uri,
      task: Task {
        pid: resp.into_inner().pid,
        container_id: None,
        running: false
      },
      mutex: parking_lot::Mutex::new(parallel_invokes as i32),
      function: None
    })
  }

  /// run_container
  /// 
  /// creates and starts the entrypoint for a container based on the given image
  /// Run inside the specified namespace
  /// returns a new, unique ID representing it
  pub async fn run_container(&mut self, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: u32) -> Result<Container> {
    info!("Creating container from image '{}', in namespace '{}'", image_name, namespace);
    let mut container = self.create_container(image_name, namespace, parallel_invokes, mem_limit_mb).await?;
    let mut client = TasksClient::new(self.channel());
  
    let req = StartRequest {
      container_id: container.container_id.clone(),
      ..Default::default()
    };
    let req = with_namespace!(req, namespace);
  
    let resp = client.start(req).await?;
  
    debug!("Task {}: {:?} started", container.container_id, resp);
    container.task.running = true;
    Ok(container)
  }

  /// Removed the specified container in the namespace
  pub async fn remove_container(&mut self, container_name: String, namespace: String) -> Result<()> {
    self.connect().await?;
    let mut client = ContainersClient::new(self.channel());

    let req = DeleteContainerRequest {
        id: container_name.clone(),
    };
    let req = with_namespace!(req, namespace);

    let _resp = client
        .delete(req)
        .await?;

    // TODO: delete / release namespace here
    debug!("Container: {:?} deleted", container_name);
    Ok(())
  }

  /// Ensures that the specified image is available on the machine
  pub async fn ensure_image(&mut self, image_name: &String) -> Result<()> {
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

}
