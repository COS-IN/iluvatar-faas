# Investigation: Shared DSQ tasks not being consumed

```
            ┌─────────────────────────────┐
            │                             ▼
   Capture Observations              Ask Questions
            ▲                             │
            └─────────────────────────────┘
                Until Root Cause is Found
```

### Observation 0 

  * dispatch callback is being called on the constrained cores 
```
   async-process-2718029 [003] d..31 3682834.319148: bpf_trace_printk: [info-constrained] [____constrained_dispatch] on cpu -3-
          <idle>-0       [003] dN.31 3682834.820117: bpf_trace_printk: [info-constrained] [____constrained_dispatch] on cpu -3-
     kworker/3:2-2663425 [003] d..31 3682834.820231: bpf_trace_printk: [info-constrained] [____constrained_dispatch] on cpu -3-
``` 

  * tasks are being put on the shared dsq in enqueue 
```
  2718228-gunicorn to shared dsq with slice 1000000 ns with flags 0x49
```

  * consumed is only successful once on cpu 3 
    * all the other times it fails 
```
  <idle>-0       [003] dN.31 3683798.859330: bpf_trace_printk: [info-constrained] ____constrained_dispatch consume failed on cpu: 3
  kworker/3:2-2663425 [003] d..31 3683798.859364: bpf_trace_printk: [info-constrained] ____constrained_dispatch consume failed on cpu: 3
  <idle>-0       [002] dN.31 3683799.024906: bpf_trace_printk: [info-constrained] ____constrained_dispatch consume failed on cpu: 2
  kworker/2:1-3119652 [002] d..31 3683799.024943: bpf_trace_printk: [info-constrained] ____constrained_dispatch consume failed on cpu: 2
```

### Questions 0 

  * Why is consume failing for the shared dsq? 
    consume api says that it would fail if there aren't any tasks on the q to consume 
    Returns %true if a task has been consumed, %false if there isn't any task to consume.

  * What does the scx api say about the number of items on the shared dsq? 
    it says that it's 2 same as tracked idependently 
 
  * What if tasks are dispatched to the local dsqs directly?   
    * that works fine! 

### Observation 1 
  
  * schedext core would not consume a task on local dsq which is not allowed by cpumask 
```
  sudo taskset  -p 0x1 2302
  pid 2302's current affinity mask: ffffffffffff
  pid 2302's new affinity mask: 1

[abrehman@v-021] workspace $ sudo taskset  -p 0x1 2302
  fs_policy_const-2302    [012] dN.31  1370.044700: bpf_trace_printk: [2302][fs_policy_const]   cpus_ptr: OOOOOOOO|OOOOOOOO|OOOOOOOO|OOOOOOOO|OOOOOOOO|OOOOOOOO|   user_cpus_ptr:    cpus_mask:
  fs_policy_const-2302    [012] dN.31  1371.044688: bpf_trace_printk: [2302][fs_policy_const]   cpus_ptr: OOOOOOOO|OOOOOOOO|OOOOOOOO|OOOOOOOO|OOOOOOOO|OOOOOOOO|   user_cpus_ptr:    cpus_mask:
  sudo-2368    [027] d..31  1371.408944: bpf_trace_printk: [2302][fs_policy_const]   cpus_ptr: O-------|--------|--------|--------|--------|--------|   user_cpus_ptr:    cpus_mask:
  sudo-2368    [027] d..31  1371.408955: bpf_trace_printk: [2302][fs_policy_const]   cpus_ptr: O-------|--------|--------|--------|--------|--------|   user_cpus_ptr:    cpus_mask:
```

### Questions 1 

  * is it possible to force a certain cpu mask in bpf scheduler? 
    * probably not, all it can do it respect the cpu mask 
    * didn't find any api 

```
  sudo taskset  -p 0x1 2302
```

### Observation 2 
### Questions 2 
## What is the root cause of the problem? 
## How to fix it? 


