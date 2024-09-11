
# Capturing per Function Metrices  

## Why is this feature required? 

  * to enable online fine scheduling policy - that is metadata driven 
  * would help do high level analysis before sharing the refined metadata with the policy 
    * it's easier to do analysis in userspace as opposed to in a bpf program! 

### Configuration

  * populate with an existing csv 

### Mechanisms

  * dump collected metrices to a csv 
  * read a dumped csv to prepopulate the map 
  * capture data from fs or a utility output 
    * cpu usage - usr, sys times 
    * throttles ? 
  * snapshots at start of invocation / end of it 
  * post processing at the end of invocation 
    * diff
    * agg addition 
    * historic remembering ? 
  * query on fqdn basis 

### Design (Words)

  * characteristics map 
    * invoke_start( fqdn, tid )
    * invoke_end( fqdn, tid )
    
  * stats_reader
    * read( cgroupid, metric )

#### critical assumptions 
  
  * tid to cgroup associate is there 

### Design (ASCII Flow Diagram)

  * use paper it's more effective 

## Implementation Details 

### Components 

### Flows 

### Verification  


## Questions 

> Where to build and put tid to cgroup associations? 


> Is it possible to design it in a way that reading is abstracted? 

  yes! 
    reading can be from a fs file 
    or a utility 
  
  what we need is the reset of the pipeline 

> Does pcp provide more value? 

  it's comprehensive library like a holy grail of all the metrices in one place! 

> Which metrices are exposed from sysfs on per cgroup level? 

```
cpuacct.stat               cpuacct.usage_all          cpuacct.usage_percpu_sys   cpuacct.usage_sys
cpuacct.usage              cpuacct.usage_percpu       cpuacct.usage_percpu_user  cpuacct.usage_user

sys time usage - agg for the group 
  /sys/fs/cgroup/cpu,cpuacct/docker/2c16758b7639b70a349b3019791cbf6632393683e24bceda3c3b25394a5bdb94/cpuacct.usage_sys

user time usage - agg for the group 
  /sys/fs/cgroup/cpu,cpuacct/docker/2c16758b7639b70a349b3019791cbf6632393683e24bceda3c3b25394a5bdb94/cpuacct.usage_user

cpuacct.stat ?? 
  user 196
  system 33

cpuacct.usage - sum of sys and user ??  
  2301206527

cpuacct.usage_sys
  328000000

cpuacct.usage_user
  1916000000

cpuacct.usage_all
    cpu user system
    0 8000000 0
    1 0 0
    2 0 0
    3 0 0
    4 0 0
    5 0 0
    6 0 0
    7 0 0
    8 0 0
    9 1520000000 264000000
    10 0 0
    11 340000000 52000000
    12 0 0
    13 0 0
    14 0 0
    15 0 0
    16 0 0
    17 0 0
    18 32000000 8000000
    19 0 0
    20 0 4000000
    21 12000000 4000000
    22 0 0
    23 0 0
    24 0 0
    25 0 0
    26 0 0
    27 0 0
    28 0 0
    29 0 0
    30 0 0
    31 0 0
    32 0 0
    33 0 0
    34 0 0
    35 0 0
    36 0 0
    37 0 0
    38 4000000 0
    39 0 0
    40 0 0
    41 0 0
    42 0 0
    43 0 0
    44 0 0
    45 0 0
    46 0 0
    47 0 0
```

> Would it be easier to 
  > read it from sysfs and do postprocessing in rust
    > or
  > use pcp rust binding to get the same info? 


> How to correlate the invocation tid with cgroup id? 


























