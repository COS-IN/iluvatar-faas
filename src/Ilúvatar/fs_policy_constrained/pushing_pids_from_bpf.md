
# Pushing PIDs from BPF scheduler to userspace 

## What are we designing for? 
  
  switching sched policy of functions tasks

  Why?
    
    existing solution does not gurantee timing and complete correctness  
    
    whereas this would gurantee timing 
      to overhead of kicking the user space scheduler thread

    guarantee timing 
      by virtue of the fact that we would be checking the cgroup structures  

## Components and Flow 
  
  bpf scheduler  
    
    detects if pid belongs to a cgroup 
    pushes the pid to a ring buffer 

  user space thread 
    
    pulls the pid from the ring buffer 
    call syscall on it 

## Design Diagram 

```
                                                                                ┌──────┐       makes syscall                                  
                                                        ┌────────────────────►  │      │  ────────────────────┐                               
                                                        │                       │      │                      │                               
                                                        │                       └──────┘                      │                               
                                                        │                                                     │                               
                                                        │                       User Space                    │                               
                                                        │                        Thread                       │                               
                                                        │                                                     │                               
                                                            ring buffer                                       │                               
                                                     ┌─────┐                                                  ▼   User Space                  
                            ─────────────────────────│─────│────────────────────────────────────────────────────────────────────────────      
                                                     └──▲──┘                                                                                  
   ┌────────────┐ ✓                                     └────────────┐                                            BPF Scheduler               
   │Static list │                                                    │     User Space                                                         
   │of funcs    │                                                    │                                                                        
   │            │                                  Call Backs        │      Thread                                                            
   │pyaes       │                                              ✓     │      ┌───────┐        ┌──────────────┐                                 
   │            │    ─────────────────────────────►  cgroup_init ◄┐  │      │       ▼        ▼              │                                 
   │rodina*     │             ┌──────────────────────             │  │      │      ┌─┐      ┌─┐             │                                 
   │            │             │                                   │  │      │      │ │      │ │             │                                 
   └────────────┘             │                  ┌─► constrained_enqueue    │      │ │      │ │  Func       │                                 
   ┌──────────────────┐       │                  │      ▲         │         │      │ │      │ │             │                                 
   │ HashMap ✓        │  ◄────┘                  │      │         │         │      │ │      │ │  Tasks      │                                 
   │                  │                          │      │         │         │      └┬┘      └┬┘             │                                 
   │   groupid, name  │                          │      │         │         │       │        ┼─────┬────┐   │                                 
   │                  │   ───────────────────────┼──────┘         │         │       ▼        ▼     ▼    ▼   │                                 
   │                  │     (check groupid existence)             │         │      xx       xx    xx   xx   ▲                                 
   │                  │                          │                │         └───   xx       xx    xx   xx ──┘                                 
   └──────────────────┘                          │                │                                                                           
                  ───────────────────────────────┼────────────────┼─────────────────────────────────────────────────────────────────────      
                                                 │                │                                                Kernel Protected Area      
                                                 │                │(name copy)                                                                
                                                 │                │                                                                           
                                                 │     cgroup structure                                                                       
                                                 │                                                                                            
                                                 │                                                                                            
                                        (name copy) ?                                                                                         
                                                 │     task structure                                                                         
                                                                                                                                              
```

                ✓



# Investigation: tasks policy changing to schedext by constrained without sched syscall   
```
            ┌─────────────────────────────┐
            │                             ▼
   Capture Observations              Ask Questions
            ▲                             │
            └─────────────────────────────┘
                Until Root Cause is Found
```
## Process 
### Observation 0 
  
  * enqueue is seeing gunicorn tasks for gzip and their policy is changed to #7 
    * even though no sched syscall has been made 
  * but the last thread policy is still TS ( SCHED_OTHER )

### Questions 0 

  Does last thread show up in init_task? 
    yes it does! 

### Observation 1 
### Questions 1 
### Observation 2 
### Questions 2 
## What is the root cause of the problem? 
## How to fix it? 




