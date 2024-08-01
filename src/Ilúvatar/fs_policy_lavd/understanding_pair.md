# Understanding: Pair Policy  

## What is the context? (read)
  
  Understand how BPF pair policy is implemented so that 
  I can implement my own policy easily. 

## What is the summary of what I just read? (read)

  This policy enforces execution on cpu pairs for the same cgroup. 
  
  Policy creates pair of cpus based on the given stride. Pairs only schedule 
  tasks belonging to the same cgroup. 
  
### Scheduler initialization 
    
  during initialization cpus are paired, pair ids are created 
  in-pair-index is assigned and pair_ctx struct is created. 

  all this is constant initialization step 

### Tasks and Cgroups  

  a new cgroup is registered using pair_cgroup_init callback 
  every task is always associated with one cgroup 
  as the task is enqueue it get's added to it's cgroup fifo queue 
    cgroup ID from task struct is used to lookup the fifo queue for placement 

  cgrp_q_idx_hash BPF hash map -- maps cgroup ID -> globally unique ID 

  two maps - fifo queue and it's lenght per cgroup 

### Dispatching tasks 

  pair_dispatch callback  

    fetch the pair struct - which is used to sync pair cpus 
      
      sync happens whenever any of the following events occur 
        
        * cgroup slice expires 
        * cgroup becomes empty 
        * any of the cpu in pair is prempted by a higher priority sched class 
      
        in case of any of these events - pair transitions to a drainning state 
        and stops executing new tasks 
    
    if the other cpu in the pair is still executing tasks - mark as draining and wait 
    for the other cpu to stop executing the task 

    otherwise, fetch new cgroup id from top_q  

    pop a task from that's cgroup's fifo queue and start executing it's task

### Dealing with preemption 

  pair_cpu_release    
    mark prempted in pair struct 
    cpu preempted -> IPI to pair cpu using scx_bpf_kick_cpu(pair_cpu, SCX_KICK_PREEMPT | SCX_KICK_WAIT); 
  
  pair_cpu_acquire 
    mark unprempted in pair struct 
    cpu acquired -> IPI to pair cpu using scx_bpf_kick_cpu(pair_cpu, SCX_KICK_PREEMPT | SCX_KICK_WAIT); 

### Visual Summary of all the items 

```
    ┌───────────────────────────────────────────────────┐                                                
    │                                                   │                                                
    │  pairs    are always synchronized                 │                                                
    │                                                   │                                                
    │  pair_ctx        0,1  2,3  4,5  6,7  8,9  10,11   │                                                
    │                                                   │                                                
    └───────────────────────────────────────────────────┘                                                
                             ▲                                        ┌──┐                               
                             │                                        │┼┼│                               
                             │                     cgrp_q_idx_hash    │┼┼│ Consulted when                
                             │                                        │┼┼│  adding to fifos              
                    ┌────────┴──────────┐                             │┼┼│  taking from fifos            
                    │ Picking Logic     │                             │┼┼│                               
                    │                   │                             └──┘                               
                    │                   │ ◄──────────────────┐                                           
                    │                   │                    │                                           
                    │                   │                   ┌┴┐                                          
                    └───────────────────┘                   │ │   cgroup id queues                       
                              ▲                      topq   │ │                                          
                              │                             │ │      used to pick next cgroup            
                              │                             └─┘                                          
     ┌────────────────────────┴─────────────────┐            ▲      for which tasks should be scheduled  
     │                                          │            │                                           
     │             ┌─┐  ┌─┐  ┌─┐  ┌─┐  ┌─┐  ┌─┐ │            │                                           
     │  cgroup     │ │  │ │  │ │  │ │  │ │  │ │ │            └───┐                                       
     │   queues    │ │  │ │  │ │  │ │  │ │  │ │ │                │                                       
     │             └─┘  └─┘  └─┘  └─┘  └─┘  └─┘ │                │                                       
     │  fifos                                   │                │                                       
     └──────────────────────────────────────────┘                │                                       
                                     ▲                       ┌───┴────────────┐                          
                                     │                       │                │                          
                                     │                       │  new enqueued  │                          
                                     │                       │     task       │                          
                                     └────────────────────── │                │                          
                                                             └────────────────┘                          
                                                                                                         
``` 

```
        ┌──────────────────────┐
        │                      ▼
   Ask Questions       Answer Questions
        ▲                      │
        └──────────────────────┘
         Until no more questions
                 or
               Timeout
```

## Questions (Reasoning)

  What does the pair_ctx represent? Why?  
    current executing state of the cpu pair
    to keep track of both cpus in the pair 

  How are fifo queues implemented? 
    simple bpf map queue 

  How is topq implemented? 
    bpf map of type queue whose entries are u64  

  How is hashmap for cid to guid implemented? 
    it's a bpf hash map with 
      key -- cgroup id 
      value -- index into array of cgroup fifos (cgrp_q)

  How are unique ids generated? 
    an array of busy states is created to remember which fifo q is taken  
    when a new cgroup is to be created 
      that busy array is parsed to find an empty slot 
      mark it as busy 
      remember the slot in haspmap 

  How are cgroup queues created? 
    bpf map - array of maps  

  How does enqueue function? 
    get array index from cgroup id  
    push the pid of the task into the fifo of cgroup 
    it's the first task of the cgroup - push cgroup id to the top_q

  Why is just pid remembered in enqueue? 
    to avoid having to duplicate the task struct - probably 

  How is pid converted to task? 


  When are items added to topq? 
    in enqueue 

  When is an item drained from toqq? 
  
  What happens when a task running? Is it added to the cgroup fifo? 
  What happens when a task stops? Is it added back to the cgroup fifo? 
  
  What are the time slices for each task? 
  How does select_cpu behave? 

  What will happen if an existing task switches to sched_ext? 
  
  What is cgroup local storage in bpf context? 
    apparently, it can only be accessed by bpf programs attached to the cgroup


## What are the fundamentals? (reason)

## Why? 

## Why? 

## Why? 

## What level of understanding do I have?

