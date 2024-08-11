
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
   ┌────────────┐                                       └────────────┐                                            BPF Scheduler               
   │Static list │                                                    │     User Space                                                         
   │of funcs    │                                                    │                                                                        
   │            │                                  Call Backs        │      Thread                                                            
   │pyaes       │                                                    │      ┌───────┐        ┌──────────────┐                                 
   │            │    ─────────────────────────────►  cgroup_init ◄┐  │      │       ▼        ▼              │                                 
   │rodina*     │             ┌──────────────────────             │  │      │      ┌─┐      ┌─┐             │                                 
   │            │             │                                   │  │      │      │ │      │ │             │                                 
   └────────────┘             │                  ┌─► init_task ───┼──┘      │      │ │      │ │  Func       │                                 
   ┌──────────────────┐       │                  │      ▲         │         │      │ │      │ │             │                                 
   │ HashMap          │  ◄────┘                  │      │         │         │      │ │      │ │  Tasks      │                                 
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



