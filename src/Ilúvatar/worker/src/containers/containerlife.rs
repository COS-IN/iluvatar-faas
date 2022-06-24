use std::collections::HashMap;
use std::sync::Arc;
use client::services::v1::containers_client::ContainersClient;
use client::services::v1::tasks_client::TasksClient;
use client::tonic::Code;
use guid_create::GUID;
use iluvatar_lib::utils::{Port, temp_file};
use log::{debug, warn, info};
use iluvatar_lib::bail_error;
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
use client::services::v1::{CreateContainerRequest, CreateTaskRequest, StartRequest, DeleteContainerRequest, DeleteTaskRequest, KillRequest};
use client::services::v1::Container as Containerd_Container;
use client::services::v1::{GetImageRequest, ReadContentRequest};
use client::services::v1::container::Runtime;
use client::with_namespace;
use containerd_client::tonic::Request;
use prost_types::Any;
use std::process::Command;
use crate::containers::structs::{Container, Task};
use crate::network::namespace_manager::NamespaceManager;
use crate::network::network_structs::Namespace;
// use std::io::Write;

use super::structs::RegisteredFunction;

#[derive(Debug)]
pub struct ContainerLifecycle {
  channel: Option<Channel>,
  namespace_manager: Arc<NamespaceManager>,
}

/// A service to handle the low-level details of container lifecycles:
///   creation, destruction, pulling images, etc
///   
impl ContainerLifecycle {
  pub fn new(ns_man: Arc<NamespaceManager>) -> ContainerLifecycle {
    // let temp_file = iluvatar_lib::utils::temp_file(&"resolv".to_string(), "conf")?;
    // let mut file = std::fs::File::create(temp_file)?;
    // let bridge_json = include_str!("../resources/cni/resolv.conf");
    // writeln!(&mut file, "{}", bridge_json)?;

    ContainerLifecycle {
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
  fn spec(&self, host_addr: &str, port: Port, mem_limit_mb: u32, cpus: u32, net_ns_name: &String) -> Any {
    let spec = include_str!("../resources/container_spec.json");
    let spec = spec
        .to_string()
        .replace("$ROOTFS", "rootfs")
        .replace("$OUTPUT", "")
        .replace("$HOST_ADDR", host_addr)
        .replace("$PORT", &port.to_string())
        .replace("$NET_NS", &NamespaceManager::net_namespace(net_ns_name))
        .replace("\"$MEMLIMIT\"", &(mem_limit_mb*1024*1024).to_string())
        .replace("\"$CPUSHARES\"", &(cpus*1024).to_string());
        // .replace("$RESOLVCONFPTH", "/home/alex/repos/efaas/src/Ilúvatar/worker/src/resources/cni/resolv.conf"); // ../resources/cni/resolv.conf
        // {
        //   "destination": "/etc/resolv.conf",
        //   "source": "$RESOLVCONFPTH",
        //   "options": [
        //     "ro",
        //     "bind"
        //   ]
        // },
        // .replace("$HOSTSPTH", "/home/alex/repos/efaas/src/Ilúvatar/worker/src/resources/cni/hosts");
        // {
        //   "destination": "/etc/hosts",
        //   "source": "$HOSTSPTH",
        //   "options": [
        //     "ro",
        //     "bind"
        //   ]
        // },
    
    Any {
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
  
  /// Read through an image's digest to find it's snapshot base
  pub async fn search_image_digest(&self, image: &String, namespace: &str) -> Result<String> {
    // Step 1. get image digest
    let get_image_req = GetImageRequest { name: image.into() };
    let mut cli = ImagesClient::new(self.channel());
    let rsp = match cli.get(with_namespace!(get_image_req, namespace)).await {
        Ok(rsp) => rsp.into_inner(),
        Err(e) => {
          bail_error!("Failed to prepare snapshot and load mounts: {:?}", e);
        },
    };
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
        let config_index: ImageIndex = match serde_json::from_slice(&content) {
            Ok(s) => s,
            Err(e) => bail_error!("JSON error getting ImageIndex: {}", e),
        };
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
        let layer_item: ImageManifest = match serde_json::from_slice(&self.read_content(namespace, manifest_item).await?) {
            Ok(s) => s,
            Err(e) => bail_error!("JSON error getting ImageManifest: {}", e),
        };
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
    let config: ImageConfiguration = match 
        serde_json::from_slice(&self.read_content(namespace, layer_item.digest().to_owned()).await?) {
          Ok(s) => s,
          Err(e) => bail_error!("JSON error getting ImageConfiguration: {}", e),
      };
  
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
  pub async fn load_mounts(&self, id: &str, snapshot_base: &String) -> Result<Vec<containerd_client::types::Mount>> {
    let view_snapshot_req = PrepareSnapshotRequest {
        // TODO: be picky about snapshotter?
        // https://github.com/containerd/containerd/tree/main/docs/snapshotters
        snapshotter: "overlayfs".to_string(),
        key: id.to_owned(),
        parent: snapshot_base.clone(),
        labels: HashMap::new(),
    };
    let mut cli = SnapshotsClient::new(self.channel());
    let rsp = cli
        .prepare(with_namespace!(view_snapshot_req, "default"))
        .await;
    if let Ok(rsp) = rsp {
      let rsp = rsp.into_inner();
      debug!("got mounts {} {}", id, rsp.mounts.len());
      Ok(rsp.mounts)
    } else {
      bail_error!("Failed to prepare snapshot and load mounts: {:?}", rsp);
    }
  }

  /// Create a container using the given image in the specified namespace
  /// Does not start any process in it
  pub async fn create_container(&self, fqdn: &String, image_name: &String, namespace: &str, parallel_invokes: u32, mem_limit_mb: u32, cpus: u32, reg: &Arc<RegisteredFunction>) -> Result<Container> {
    let port = 8080;

    let cid = format!("{}-{}", fqdn, GUID::rand());
    let ns = self.namespace_manager.get_namespace()?;

    let address = &ns.namespace.ips[0].address;

    let spec = self.spec(address, port, mem_limit_mb, cpus, &ns.name);

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
              bail_error!("Containerd failed to create container with error: {}", e);
            },
        };

    debug!("Container: created {:?}", resp);

    let mounts = self.load_mounts(&cid, &reg.snapshot_base).await?;

    let req = CreateTaskRequest {
        container_id: cid.clone(),
        rootfs: mounts,
        checkpoint: None,
        options: None,
        // TODO: clean up these files on container/task deletion
        stdin: temp_file(&cid, "stdin")?,
        stdout: temp_file(&cid, "stdout")?,
        stderr: temp_file(&cid, "stderr")?,
        terminal: false,
    };
    let req = with_namespace!(req, namespace);
  
    let mut client = TasksClient::new(self.channel());
    match client.create(req).await {
        Ok(t) => {
          debug!("Task: created {:?}", t);
          let task = Task {
            pid: t.into_inner().pid,
            container_id: None,
            running: false
          };
          Ok(Container::new(cid, task, port, address.clone(), parallel_invokes, &fqdn, &reg, ns))
        },
        Err(e) => {
          self.remove_container(&cid, &ns, namespace).await?;
          bail_error!("Create task failed with: {}", e);
        },
    }
  }

  /// run_container
  /// 
  /// creates and starts the entrypoint for a container based on the given image
  /// Run inside the specified namespace
  /// returns a new, unique ID representing it
  pub async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: u32, cpus: u32, reg: &Arc<RegisteredFunction>) -> Result<Container> {
    info!("Creating container from image '{}', in namespace '{}'", image_name, namespace);
    let mut container = self.create_container(fqdn, image_name, namespace, parallel_invokes, mem_limit_mb, cpus, reg).await?;
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
          Ok(container)
        },
        Err(e) => bail_error!("Starting task failed with {}", e),
    }
  }

  /// Removed the specified container in the namespace
  pub async fn remove_container(&self, container_id: &String, net_namespace: &Arc<Namespace>, namespace: &str) -> Result<()> {
    let mut client = TasksClient::new(self.channel());

    let req = KillRequest {
      container_id: container_id.clone(),
      // exec_id: container.task.pid.to_string(),
      exec_id: "".to_string(),
      signal: 9, // SIGKILL
      all: true,
    };
    let req = with_namespace!(req, namespace);

    let resp = client
        .kill(req)
        .await;
    match &resp {
        Ok(_) => {},
        Err(e) => {
          if e.code() == Code::NotFound {
            // task crashed and was removed
            warn!("Task for container '{}' was missing when it was attempted to be killed", container_id);
          } else {
            bail_error!("Attempt to kill task in container '{}' failed with error: {}", container_id, e);
          }
        },
    };
    debug!("Kill task response {:?}", resp);

    let req = DeleteTaskRequest {
      container_id: container_id.clone(),
    };
    let req = with_namespace!(req, namespace);

    let resp = client
        .delete(req)
        .await;
    match &resp {
      Ok(_) => {},
      Err(e) => {
        match e.code() {
                      // task crashed and was removed
          Code::NotFound => warn!("Task for container '{}' was missing when it was attempted to be delete", container_id),
          _ => bail_error!("Attempt to delete task in container '{}' failed with error: {}", container_id, e),
        }
      },
    }
    debug!("Delete task response {:?}", resp);

    let mut client = ContainersClient::new(self.channel());

    let req = DeleteContainerRequest {
        id: container_id.clone(),
    };
    let req = with_namespace!(req, namespace);

    let _resp = match client
        .delete(req)
        .await {
            Ok(resp) => resp,
            Err(e) => {
              bail_error!("Delete container failed with error {}", e);
            },
        };
    debug!("Delete container response {:?}", _resp);

    // TODO: delete / release namespace here
    self.namespace_manager.return_namespace(net_namespace.clone())?;

    debug!("Container: {:?} deleted", container_id);
    Ok(())
  }

  /// Ensures that the specified image is available on the machine
  pub async fn ensure_image(&self, image_name: &String) -> Result<()> {
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
