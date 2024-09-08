
# Capturing Cgroup id of a function 

  * docker backend 
    * capture using inspect cmd                 ✓
    * save it somewhere so that it can be used when adding to the map 


# Relating kn->id, cgroup 128 bit id 
  
  * kn->id is the inode id 
    * to corelate the inode id to cgroup id 
      * docker backend 
        * kn->comm -- name 
        * userpace -> name from docker ps 
      * cgrouphashmap -> key: kn->id, value: kn->comm
      * func_chars: key: userspace name as u64 value: metadata 
      
```
bpf_trace_printk : [info-tsksz] [cgroup-id] converted key                                                     : 969526364333643008 p->sched_task_group->css.cgroup->kn->id : 8735
bpf_trace_printk : [info-tsksz] [cgroup-id][chashmap_insert] -- 1920701696 -OK-inserted- d747317727b9500, qid : 0
bpf_trace_printk : [info-tsksz] [cgroup-id][map][func_characs] key                                            : 969526364333643008 e2e                                     : 0
```

## Verification 




