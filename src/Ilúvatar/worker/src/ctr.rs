use containerd_client as client;

use client::services::v1::container::Runtime;
use client::services::v1::containers_client::ContainersClient;
use client::services::v1::images_client::ImagesClient;
use client::services::v1::Container;
use client::services::v1::{CreateContainerRequest, DeleteContainerRequest, ListImagesRequest};

use client::services::v1::tasks_client::TasksClient;
use client::services::v1::{CreateTaskRequest, DeleteTaskRequest, StartRequest, WaitRequest};

use prost_types::Any;

use tonic::Request;

use std::pin::Pin;
    
use std::fs;
use std::fs::File;
use futures::executor::block_on;

use client::with_namespace;

const CID: &str = "abc123";
const NAMESPACE: &str = "default";

#[tokio::main(flavor = "current_thread")]
async fn ctr_creat() {
    let containerd_unix_sock = "/run/containerd/containerd.sock" ;
    let channel = client::connect(containerd_unix_sock).await.expect("Cant connect to containerd");
    
    // let mut client = ImagesClient::new(channel);
    // let imgreq = ListImagesRequest::default();    
    // let ireq = with_namespace!(imgreq, NAMESPACE);
    // let resp = client
    //     .list(ireq)
    // 	.await
    //     .expect("Failed to list images");

    // let imglist = resp.into_inner().images;
    // let firstimg = &imglist[0].name; //docker.io/library/redis:alpine
    
    // println!("{:?}", firstimg);

    let mut client = ContainersClient::new(channel.clone());
    let firstimg = "docker.io/library/redis:alpine";

    let rootfs = "rootfs" ; //"/tmp/busybox/bundle/rootfs";
    //Uncertain about this https://github.com/ktock/containerd/blob/894b81a4b802e4eb2a91d1ce216b8817763c29fb/oci/spec.go#L132
    
    // the container will run with command `echo $output` in args 
    let output = ""; //"hello rust client";

    // The spec is compulsory. containerd has oci spec generation helpers, but not accessible via the API?
    // Can also get this via `ctr oci spec`
    
    let spec = include_str!("container_spec.json");
    let spec = spec
        .to_string()
        .replace("$ROOTFS", rootfs)
        .replace("$OUTPUT", output);
    
    let spec = Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: spec.into_bytes(),
    };

    let container = Container {
        id: CID.to_string(),
        image: firstimg.to_string(), //"docker.io/library/alpine:latest".to_string(),
        runtime: Some(Runtime {
            name: "io.containerd.runc.v2".to_string(),
            options: None,
        }),
        spec: Some(spec),
        ..Default::default()
    };

    let req = CreateContainerRequest {
        container: Some(container),
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client
        .create(req)
        .await
        .expect("Failed to create container");

    println!("Container: {:?} created", CID);

    //start processes inside the container 
    // creat and start task
    let mut client = TasksClient::new(channel.clone());

    let req = CreateTaskRequest {
        container_id: CID.to_string(),
        // stdin: stdin.to_str().unwrap().to_string(),
        // stdout: stdout.to_str().unwrap().to_string(),
        // stderr: stderr.to_str().unwrap().to_string(),
        ..Default::default()
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client.create(req).await.expect("Failed to create task");

    println!("Task: {:?} created", CID);

    let req = StartRequest {
        container_id: CID.to_string(),
        ..Default::default()
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client.start(req).await.expect("Failed to start task");

    println!("Task: {:?} started", CID);

    

    // Deletion 
    let mut client = ContainersClient::new(channel);

    let req = DeleteContainerRequest {
        id: CID.to_string(),
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client
        .delete(req)
        .await
        .expect("Failed to delete container");

    println!("Container: {:?} deleted", CID);

//    my_list_images(channel.clone());
    
}
