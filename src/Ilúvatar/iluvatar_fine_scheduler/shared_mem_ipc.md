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

## Description

## Questions 

  Does scheduler allow use of shared memory library given it's allocator limitation? 
    ??? 

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














