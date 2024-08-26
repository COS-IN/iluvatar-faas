# Power of Two 

Preserving Locality 
  Each time a new cgroup is created it is assigned a specific Q for it's lifetime.
  All tasks spawned in that cgroup are pushed to the assigned Q. 
  It helps to preserve locality. 

Power of two  
  Each Q is inturn associated with only 2 cores. 
  Whichever core is idle is used to execute the next task in the Q for a default timeslice. 

Qs operate in fifo manner with tasks whose timeslice expires are pushed back of the Q.   

# Implementation Details 

## Assigning specific Q to a Specific Cgroup 

  cgroup id to Q id 
    hashmap 

  whenever a new cgroup is discovered 
    it's assigned a new Q id in a round robin fashion 
 
## Getting a new Q id for the new cgroup 
  
  round robin - Q id generation 

## Dispatching tasks  

  current cpu -> Q id -> task pulled 
  cpu to qid function 

## Components 

  cgroupid to qid - using existing CgroupsHashMap 

  funcs
    qid generator 
    cpu_to_qid

## flows 
  
  init_task                 ✓
    put the new task into specific Q  

  select_cpu                 ✓
    cgroup id -> qid -> get idle cpu among two cpus  
    if idle is available 
      dispatch to it's local dsq 
      and return the cpu 
    else 
      return cpu_prev 

  enqueue                 ✓
    cgroup id -> qid
    put in the destination Q 

  dispatch                 ✓
    cpu -> qid 
    consume the shared Q 

## Verification  

  Is cgroup captured? 
```
  sudo cat /sys/kernel/debug/tracing/trace_pipe | grep -i init_task | grep -i '-RG-'
```

  Are qids assigned to cgroups in a round robin fashion? 
```
   sudo cat /sys/kernel/debug/tracing/trace_pipe | grep -i consumed
   ./single_func/invocations.sh
   myocyte.out-47525   [000] d..31 141581.276072: bpf_trace_printk: [info-powof2] ____powof2_dispatch consumed a task from Q 0
```
   show that the tasks are being consumed from Q 0 only 
  
   creating another function and making it's invocation show that 
   it's tasks are consumed from Q 1 ? 
```
  <idle>-0       [004] dN.31 142023.257050: bpf_trace_printk: [info-powof2] ____powof2_dispatch consumed a task from Q 2
  cat-52244   [003] d..31 142023.293572: bpf_trace_printk: [info-powof2] ____powof2_dispatch consumed a task from Q 1
```

  Are tasks pushed to only assigned Qs? 
    in above examples, Q0 for myocyte and then Q2 for gzip 

  Are tasks being consumed on the target CPUs only? 
    yes 
```
  myocyte.out-58089   [000] d..41 142522.403656: bpf_trace_printk: [info-powof2] [____powof2_select_cpu] selected cpu 1 for 58062 - myocyte.out
  myocyte.out-58061   [001] d..41 142522.415651: bpf_trace_printk: [info-powof2] [____powof2_select_cpu] selected cpu 0 for 58077 - myocyte.out
```
    





