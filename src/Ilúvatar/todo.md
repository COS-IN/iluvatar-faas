# TODOs 

## Add logic to avoid making a pull request when using docker backend. 

```
  diff --git "a/src/Il\303\272vatar/iluvatar_worker_library/src/services/containers/docker/docker.rs" "b/src/Il\303\272vatar/iluvatar_worker_library/src/services/containers/docker/docker.rs"
  index 968094d..a8fcd02 100644
  --- "a/src/Il\303\272vatar/iluvatar_worker_library/src/services/containers/docker/docker.rs"
  +++ "b/src/Il\303\272vatar/iluvatar_worker_library/src/services/containers/docker/docker.rs"
  @@ -317,6 +317,8 @@ impl ContainerIsolationService for DockerIsolation {
               return Ok(());
           }

  +        // add logic to avoid making the pull call if image is already there
  +
           let output = execute_cmd_async("/usr/bin/docker", vec!["pull", rf.image_name.as_str()], None, tid).await?;
           match output.status.code() {
               Some(0) => (),
```

