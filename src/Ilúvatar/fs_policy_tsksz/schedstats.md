# Stats outputed by the policy 

## Why is this feature required? 
  
  To get an Observability into what's happening in the policy. 

### Configuration
  
  a metric to be outputed 

### Mechanisms
  
  capture metrics in the bpf scheduler as it's running 
    in a structure 
  
  have the userspace thread read that structure on each run and output it in a 
  proper format 

### Design (Words)
  

### Design (ASCII Flow Diagram)

## Implementation Details 
### Components 
  
  struct stats_Q {
    # of tasks 
  }
  hashmap of stats_Q - key - qid 

  struct stats_bucket {
    # of tasks 
  }
  hashmap of stats_bucket - key - bucket id  

  struct stats_cgroup {
    # of tasks 
  }
  hashmap of stats_cgroup - key - cgroup_name   

  global variable of this structure  

  atomic updates to this structure in the bpf scheduler 

  mechanisim to read the snapshot of this structure from user space 

### Flows 

  init_task -> updates to tsks_Q - should be atomic 
  chashmap_insert -> update to # of cgroups - should be atomic  


### Verification  


## Next Actions 

  * capture on BPF side                 ✓
  * push to the user space via ring buffer                 ✓
  * dump into a json or a csv 
    * no need we can post process the sched logs 

## Questions
  
  What is the mechanisim to read maps from the user space? 
    it's best to use the ring buffers to avoid the race issues  



