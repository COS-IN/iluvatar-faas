# Configuration

## Worker configuration

```json

{
  // name for the server
  "name": "local",
  // address to listen on
  "address": "127.0.0.1",
  // port to listen on
  "port": 8079,
  // request timeout length in seconds
  "timeout_sec": 6000,
  // limits to place on an invocation
  "limits" : {
    // invocation length timeout
    "timeout_sec": 6000,
    // minimum memory allocation
    "mem_min_mb": 5,
    // maximum memory allocation
    "mem_max_mb": 2048,
    // max number of cpus allocation
    "cpu_max": 10
  },
  "logging": {
    "level": "debug",
    // dir to store logs in
    "directory": "./logs",
    // filename start string
    "basename": "worker"
  },
  // resources the worker has to allocate to resources
  "container_resources": {
    // total memory pool in MB
    "memory_mb": 20480,
    // number of cores it can use, or number of concurrent functions to allow at once
    "cores": 4,
    // eviction algorithm to use
    "eviction": "LRU",
    // timeout on container startup before error
    "startup_timeout_ms": 10000,
    // amount of memory the container pool monitor will try and maintain as a buffer (eager eviction)
    "memory_buffer_mb": 0,
    // how often the container pool monitor will run
    "pool_freq_sec": 5
  },
  "networking": {
    // bridge name to create
    "bridge": "IlWorkBr0",
    // path to cnitool executable
    "cnitool": "/home/alex/.gopth/bin/cnitool",
    // dir with cni tool plugins
    "cni_plugin_bin": "/opt/cni/bin",
    // name of cni json file for bridge setup
    "cni_name": "il_worker_br",
    // use a pool of network namespaces
    "use_pool": false,
    // number of free namespaces to keep in the pool
    "pool_size": 10,
    // frequency of namespace pool monitor runs
    "pool_freq_sec": 1
  }
}

```