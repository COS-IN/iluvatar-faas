use std::collections::HashMap;

use client::services::v1::containers_client::ContainersClient;
use client::services::v1::tasks_client::TasksClient;
use guid_create::GUID;
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
use client::services::v1::{Container, CreateContainerRequest, CreateTaskRequest, StartRequest, DeleteContainerRequest};
use client::services::v1::{GetImageRequest, ReadContentRequest};
use client::services::v1::container::Runtime;
use client::with_namespace;
use containerd_client::tonic::Request;
use prost_types::Any;
use std::process::Command;

pub struct ContainerLifecycle {
  channel: Option<Channel>,
}

/// A service to handle the low-level details of container lifecycles:
///   creation, destruction, pulling images, etc
/// NOT THREAD SAFE
impl ContainerLifecycle {
  pub fn new() -> ContainerLifecycle {
    ContainerLifecycle {
       channel: None,
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
  fn spec(&self) -> Any {
    let spec = include_str!("../container_spec.json");
    let spec = spec
        .to_string()
        .replace("$ROOTFS", "rootfs")
        .replace("$OUTPUT", "");
    
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
    let image_digest = if let Some(image) = rsp.image {
      image.target
            .ok_or_else(|| anyhow::anyhow!("Could not find image digest"))
          .map(|v: Descriptor| v.digest)?
    } else {
      anyhow::bail!("Could not find image")
    };
  
    // println!("get image {} info {:?}", image, image_digest);
  
    // Step 2. get image content manifests
    let content = self.read_content(namespace, image_digest).await?;
    let config_index: ImageIndex = serde_json::from_slice(&content)?;
    // println!("config index = {:?}", config_index);
  
    let manifest_item = config_index
        .manifests()
        .iter()
        .find(|file| match file.platform() {
            Some(v) => v.architecture().to_string() == "amd64" && v.os().to_string() == "linux",
            None => false,
        })
        .ok_or_else(|| anyhow::anyhow!("fail to load specific manifest"))?;
  
    // Step 3. load image manifest from specific platform filter
    let layer_item: ImageManifest =
        serde_json::from_slice(&self.read_content(namespace, manifest_item.digest().to_owned()).await?)?;
  
    // Step 3. load image configuration (layer) from image
    let config: ImageConfiguration =
        serde_json::from_slice(&self.read_content(namespace, layer_item.config().digest().to_owned()).await?)?;
  
    // Step 4. calculate finalize digest
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
    // println!("load {} diff digest {}", image, prev_digest);
    Ok(prev_digest)
  }
  
  /// get the mount points for a container's (id) snapshot base
  async fn load_mounts(&mut self, id: &str, snapshot_base: String) -> Result<Vec<containerd_client::types::Mount>> {
    let view_snapshot_req = PrepareSnapshotRequest {
        // TODO: be picky about snapshotter?
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
  
    // println!("get mounts {} {}", id, rsp.mounts.len());
    Ok(rsp.mounts)
  }

  /// Create a container using the given image in the specified namespace
  /// Does not start any process in it
  pub async fn create_container(&mut self, image_name: &String, namespace: &str) -> Result<String> {
    let cid = GUID::rand().to_string();
    let spec = self.spec();

    let container = Container {
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

    let _resp = client
        .create(req)
        .await?;

    // println!("Container: created {:?}", resp);
    Ok(cid)
  }

  /// run_container
  /// 
  /// creates and starts the entrypoint for a container based on the given image
  /// Run inside the specified namespace
  /// returns a new, unique ID representing it
  pub async fn run_container(&mut self, image_name: String, namespace: &str) -> Result<String> {
    let cid = self.create_container(&image_name, namespace).await?;
    let snapshot_base = self.search_image_digest(&image_name, "default").await?;

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
    let _resp = client.create(req).await?;
    // println!("Task: started {:?}", resp);
  
    let req = StartRequest {
      container_id: cid.clone(),
      ..Default::default()
    };
    let req = with_namespace!(req, namespace);
  
    let _resp = client.start(req).await?;
  
    // println!("Task {}: {:?} started", cid, resp);

    Ok(cid)
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

    // println!("Container: {:?} deleted", container_name);
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
