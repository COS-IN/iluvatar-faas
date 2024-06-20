# Shared Memory IPC between control plane and scheduler


## What are we designing for? 

  A fast communication channel between scheduler and control plane.

  Why?
    so that stuff from control plane is immediately available to the scheduler

  Why?
    to reduce the latency from cause to effect for fine grained scheduling 

## Components 
  
  scheduler 
    server - receives data to base it's decicions on them  
    
  control plane 
    client - sends data to the server

  messages 
    characteristics packets 
      a structure with 
        func_name
        e2e 
    pid packets
      a structure with 
        pid 
        func_name

## Description
  
  Control plane puts the packets on respective channels.  
  Scheduler reads the packets from the channels and updates the map.

## Questions 

  How many items can the control plane put on the channel? 
    ? 
  
  Does scheduler allow use of shared memory library given it's allocator limitation? 
    yes  

  Why does server fail to start with root access? 
    
    because root is not allowed to establish the dbus connection 
    according to the dbus configuration 

    building a configuration file for dbus to allow root to connect to the server
    does work 

    but it causes the finescheduler to unable to find sched_ext_ops
      Error: type "sched_ext_ops" doesn't exist, ret=-2

  Can we share the shared memory configurations over a socket instead of using dbus? 
    ??? 

  Is it possible to use ipc_channel in scheduler to receive data? 
    yes string is being received fine using server 

## Issues during implementation 
  
### IPC Channel cannot be shared between threads safely 

```
   Compiling iluvatar_worker_library v0.2.0 (/data2/ar/iluvatar-faas/src/Ilúvatar/iluvatar_worker_library)
   error[E0277]: `Cell<()>` cannot be shared between threads safely
   --> iluvatar_worker_library/src/worker_api/iluvatar_worker.rs:62:25
    |
62  | impl IluvatarWorker for IluvatarWorkerImpl {
    |                         ^^^^^^^^^^^^^^^^^^ `Cell<()>` cannot be shared between threads safely
    |
    = help: within `Channels`, the trait `Sync` is not implemented for `Cell<()>`
    = note: if you want to do aliasing and mutation between multiple threads, use `std::sync::RwLock`
```

  What is the issue? 
    
    Channel uses Cell crate which doesn't have Sync trait implemented. 

    It's required by the IluvatarWorkerImpl. 

  How to fix it?
    
    Don't make it part of the structure. 

    Rather 
      * just pass it around. 
      * use static mut to make it available to all threads.


## Next Actions 
  
  * create oneshot server in control plane 
    * establish two channels between the scheduler and the cp 
      * control plane creates server and is waiting for the client  ✓ 
      * scheduler creates a connection and send channels structure
  * push stuff to the scheduler 
    * characteristics
    * pids










