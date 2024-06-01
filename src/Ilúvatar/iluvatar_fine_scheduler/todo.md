# Fine Grain Scheduling 


<!-- vim-markdown-toc Marked -->

* [Important Design Considerations](#important-design-considerations)
* [Sched_Ext in Iluvatar](#sched_ext-in-iluvatar)
  * [End goal / Deliverable (Reason)](#end-goal-/-deliverable-(reason))
  * [Next Actions (Planning)](#next-actions-(planning))
  * [Worklog (Doing)](#worklog-(doing))
  * [Review (Reading)](#review-(reading))
* [Understanding: Control Groups, PIDs and Process names](#understanding:-control-groups,-pids-and-process-names)
  * [What is the context? (read)](#what-is-the-context?-(read))
  * [What is the summary of what I just read? (read)](#what-is-the-summary-of-what-i-just-read?-(read))
  * [What are the fundamentals? (reason)](#what-are-the-fundamentals?-(reason))
  * [What level of understanding do I have?](#what-level-of-understanding-do-i-have?)
* [Overall Design](#overall-design)
  * [Flow of events](#flow-of-events)
* [Designing](#designing)
  * [CSV Reading](#csv-reading)
    * [Components](#components)
    * [Flows](#flows)
  * [Running / Stopping Eviction](#running-/-stopping-eviction)
    * [What are we designing for?](#what-are-we-designing-for?)
    * [Components](#components)
    * [Flows](#flows)
  * [Questions](#questions)
  * [Selective CPU Policy](#selective-cpu-policy)
    * [Flow of Events](#flow-of-events)
    * [Components](#components)
    * [Questions](#questions)
* [Investigations](#investigations)
  * [CPU not changing after going through dispatch call](#cpu-not-changing-after-going-through-dispatch-call)
    * [What is the root cause?](#what-is-the-root-cause?)
* [Description](#description)

<!-- vim-markdown-toc -->

## Important Design Considerations 

  * user space component must not trigger page faults 
    * all structures should be static 
      * shared memory 


## Sched_Ext in Iluvatar  

### End goal / Deliverable (Reason)
  
  Sched_ext policy should build with iluvatar 
  and should execute if configured. 

### Next Actions (Planning)
  
  * [x] build simple sched_ext policy with iluvatar 
  
  * [x] change worker to launch the scheduler process 
  * [x] setup shmem ipc between scheduler and worker 
    * [x] try the example in shmem ipc repo
      * [x] didnt' work due to dbus issue 
    * [x] change fine scheduler to dump out fd 
    * [x] try running with hardcoded values of fd 
      * [x] unable to open fd on scheduler side 
        * [x] probably because I am using the wrong fd - it's process sepcific 
        * [x] use https://docs.rs/shmem-ipc/latest/shmem_ipc/mem/mmap/struct.MmapRaw.html
        * [x] https://docs.rs/shmem-ipc/latest/shmem_ipc/mem/index.html
        * [x] raw mapping of the memory
    * use socket to share the fd with worker 
    * use shared memory to communicate 
    * measure latency of shared memory communication 
  
  * [x] understand thread structure of container function 

  * [x] implement specific functions on specific core policy 
    * [x] design 
      * [x] csv
      * [x] scheduler actions 
    * [x] update fifo policy to 
      * [x] read csv 
      * [x] read pids log  
      * [x] read priodically 
      * [x] schedule based on it 
        * [x] api to use for selecting cpu 
        * [x] api to get pid of the task 
        * [x] add logic to check pid in the pid map 
        * [x] use func_name from pid map to get the characteristics
        * [x] use the preferred core in characteristics to select the cpu 
    * [x] change the policy to schedule based on it 
    * [x] update writeup 
      * [x] to list 
        * [x] structure of threads 
        * [x] csvs 
      * [x] explain how policy works 
    * [x] showcase using worker and containers  
      * [x] doesn't work
        * [x] probably because the dispatch is not called - need to find an another way 
  
  * [x] Understand scx_layered (it handles control groups)
    * [x] read main.rs code  
    * read bpf.c code 

  * [x] solve the function threads not moving issue 
    * [x] understand 
      * Does control group threads go through the dispatch call? 
        * no! - verified using printpid in dispatch callback 
      * Why not? 
        * dispatch is only called when there are no tasks to run in the local dsq and global dsq 
      * Does this task exist in the local dsq? 
        * ?     
      * How does layered scheduler handle control group tasks? 
        * ? 

  * [x] add new callback to the fifo scheduler  
    * [x] see if I can just use the existing scheduler 
      * [x] select cpu is already there in main.bpf.c 
    * [x] Can I evict local dsqs? 
      * [x] I couldn't find any such api in kernel/sched/ext.c  
    * [x] add main.bpf.c to the scheduler 
      * [x] update it to print pid and task name of all the dispatched tasks 
    * [x] pid is being printed for the last threads in start/stop calls 
    * [x] prints in the running / stopping callbacks indicate that the threads are being dispatched

  * [x] Why isn't it going through the dispatch func? 
    * [x] How do items go through the dispatch func? 
      * [x] bpf code, submits tasks to a ring buffer in enqueue and exit callback  
      * [x] dispatch func when called, reads from the ring buffer and dispatches the task 
  
  * Why is the task only going through running and stopping callbacks? 
    * because the enqueue callback is not called for this task 

  * [x] summarize the design of fifo scheduler 
    * [x] bpf side 
    * [x] user space side 
   
  * [x] design mechanism - to schedule out a task 
    * [x] submitting pid to bpf code from user space 
    * [x] push a given pid to user queue - from stopping callback  

  * implementation - 
    * [x] link list in bpf 
      * static array of node struct pids 
      * static array of indices 
      * head points to index in next,indice array - which points to actual index in pids array 
    * [x] ringbuffer to submit pids to bpf
      * [x] build in bpf 
      * [x] user side - submit pids to ring buffer 
      * [x] bpf side - read from ring buffer and add to link list
      * [x] bpf side - on stopping callback - check if pid is in the link list 
        * [x] if so - push to dispatch ring buffer 
        * [x] remove from the link list
    * [x] But doesn't work! 

  * Why isn't CPU of hello task changed when set in dispatch func? 
    * because the select cpu callback would win instead  
 
Fri 24 May 2024 12:37:29 PM EDT

  * [x] refine implementation 
    * [x] redesign policy to pin specific tasks to specific cpus 
      * [x] previous implementation doesn't take case of select cpu callback 
      * [x] update it so that, select cpu also respects the decision 
    * [x] verify that the implementation works 
  * further implementation 
    * dump the csvs from the control plane 

  * [x] bpf program 
    * [x] add two arrays 
    * [x] active index 
    * [x] use active array in select_cpu 
  * [x] user space  
    * [x] write the inactive array  
    * [x] switch the active array index atomically - how? 

Tue 28 May 2024 05:30:09 PM EDT

  * [x] update the writeup 
    * [x] research plan to writeup 

Wed 29 May 2024 01:17:19 PM EDT

  * experimentation harness 
    * [x] setup scripts (3hrs)
      * [x] cleanup 
      * [x] worker 
      * [x] benchmarking 

Thu 30 May 2024 12:15:03 PM EDT

  * experimentation harness 
      * [x] running trace
        * [x] generating traces 
           
    * [x] post processing trace results  
      * [x] collect stuff 
      * [x] tail latency 

Fri 31 May 2024 03:33:20 PM EDT

  * [x] experimentation harness 
    * [x] collect ipmi as well 

Sat 01 Jun 2024 02:27:41 PM EDT

  * experimentation harness 
  
    * [x] plot 
      * [x] tail latency

    * update ilu 
      * config to run fine policy  
      * generate csvs 
      * run the policy 

    * policy that uses preferred cores for given functions 
      * compare plots for the two cases 




    * determine total energy 
    * plot 
      * total energy utilization 
    
    * email update  

    * capture cpu traces  
    * plot 
      * system status 


```
  client_latency_us -> e2e 
  function_output(end - start) -> code_duration_sec
  duration sent by the worker -> worker_duration_us
```

  * What should be the criteria to submit to ring buffer from running callback? 
    * if a task has been running for 1 second - submit it to the ring buffer

  * update worker 
    * to spit out function map characteristics
      * Which map? 
      * How? 
  * update scheduler 
    * to maintain a process_name to pid map 
    * to read function characteristics map 

  * change the code so that it runs when configured in worker config 
    * worker config for v-021
 
  * run perf bench to compare cfs and fifo  
    * build PID map in Iluvatar Control Plane
      * a map which has pids for each function with their characteristics 
    * change the policy to schedule based on PID map
    * rerun the perf bench to compare the three policies 



### Worklog (Doing) 



  Wed 29 May 2024 01:17:19 PM EDT
    * 3 hrs 
      * organized scripts for experimentation harness 
      * ran benchmarking 

  
  Thu 23 May 2024 11:47:33 AM EDT
    * 4 hrs  
      * debugged why dispatch was not called in the first place 
      * through prints in bpf code and reviewing ext.c and bpf.c code 
      * determined the root cause to be the select_cpu callback - which was immediately pushing stuff to local dsq
    * next: need to refine submission of epids to skip


  Wed 22 May 2024 11:41:38 AM EDT
    * 4 hrs  
      * designed and implemented the mechanism to schedule out a task
        * bpf queues to jump around the pids to be evicted 
        * ring buffer to submit pids from user space to bpf 
      * but task does not migrate to the target cpu  
        * cpumask_cnt is not updated  


  Tue 21 May 2024 11:41:38 AM EDT
    * 1 hrs 
      * completed the reading 
     

  Mon 20 May 2024 11:41:38 AM EDT
    * 4 hrs 
      * read fifo sched code 


  Fri 17 May 2024 01:18:59 PM EDT
    * 4 hrs  
      * debugged why cgroup task was not moving 
      * emailed 

  Thu 16 May 2024 10:46:56 AM EDT
    * 3 hrs 
      * updated bpf.c to be built and print pid and task names 
      * verified that 
        * gunicorn thread is not being going through the dispatch function of the main.rs   
        * but it appears in the running and stopping callbacks of the bpf 
      * next: find a way to make it go through the dispatch function

  Thu 09 May 2024 02:33:15 PM EDT
    * 2 hrs   
      * looked into why cgroup thread isn't moving 
      * never called into dispatch

  Thu 02 May 2024 09:09:02 AM EDT
    * 1.4 hrs 
      * basic reading implementation with shared state among threads 
    * 0.6 hrs
      * figured out that we cannot use threads in the scheduler code! 
      * due to the way rustland core 
    * 0.5 hrs 
      * discovered the api to use to set the cpu explicitly 
    * 1.3 hrs 
      * added logic and debugged it till it's worked 
    * 1.5 hrs 
      * writeup and demo setup 
    * total 5.5 hrs 

  Wed 01 May 2024 10:51:50 PM EDT
    
    * 3 hrs 
      * designing of csv and overall structure
      * implemented reading of csvs and list to map conversion 
      * need to correct pid csv 

  Mon 29 Apr 2024 09:28:46 AM EDT
    
    * 4 hrs 
      * tried setting up shared_memory interface 
      * but didn't work 

  Sun 28 Apr 2024 12:19:58 PM EDT
    
    * 4 hrs 
    * dbus requires x11 $display 
    * dbus mechanisim to share the shmem fd is not a good idea 
      * it requires x11 display 
    * rather use socket to share the shmem fd 
    * stripped down to use raw fd only - not working yet! 

  Sat 27 Apr 2024 03:02:59 PM EDT

    * 0.5 hrs  
      * started writing the server code in worker 
      * done with dbus setup and setting up the receiver 

  Thu 25 Apr 2024 11:18:17 AM EDT

    * 4 hrs 
      * debugged why it's cann't be part of the worker itself 
        * global allocator is replaced by rustland core 
        * it's not thread safe 
        * when worker starts setting up threads it fails at 
        * rustland allocator - stating move was already done 
      * fifo is building with iluvatar code  
      * runs when worker is executed 


### Review (Reading)

  How much time did it take? 

  What could I have done differently? 

  What worked really well? 

  What are the key lessons? 


## Understanding: Control Groups, PIDs and Process names 
### What is the context? (read)

  How does the function name in iluvatar context relate to control groups and pids? 

    What is the pid tree of a container from linux perspective?
```
  root      343157  0.0  0.0 712400  8832 ?        Sl   19:09   0:00 /usr/local/bin/containerd-shim-runc-v2 -namespace default -id hello-0.1-E063C8BA-99EB-F621-88CE-82BB8603C325 -address /run/containerd/containerd.sock
               
               (command with -id         (thread within     (thread within
               function name)            container)         container)

  systemd(1)───containerd-shim(343157)─┬─gunicorn(343177)───gunicorn(343336)
                                     ├─{containerd-shim}(343158)
                                     ├─{containerd-shim}(343159)
                                     ├─{containerd-shim}(343160)
                                     ├─{containerd-shim}(343161)
                                     ├─{containerd-shim}(343162)
                                     ├─{containerd-shim}(343163)
                                     ├─{containerd-shim}(343164)
                                     ├─{containerd-shim}(343165)
                                     ├─{containerd-shim}(343166)
                                     └─{containerd-shim}(343184)
```

```
[abrehman@v-021] workspace $ sudo ctr c ls
  CONTAINER                                                        IMAGE                                              RUNTIME

  hello-0.1-95CF7E88-566D-49C9-D2D6-BFD0621E637C                   docker.io/alfuerst/hello-iluvatar-action:latest    io.containerd.runc.v2
  hello-0.1-B190EA3E-D63C-2713-E3FE-4EF38FB7AE5D                   docker.io/alfuerst/hello-iluvatar-action:latest    io.containerd.runc.v2
  hello-0.2-A65729A3-35CD-9668-1BB3-0D66A0B167CB                   docker.io/alfuerst/hello-iluvatar-action:latest    io.containerd.runc.v2
  hello-0.2-B0A185DE-E2D8-D928-8DC4-02BB316EF728                   docker.io/alfuerst/hello-iluvatar-action:latest    io.containerd.runc.v2

  worker-health-test-1.0.0-DE719488-F5F3-9499-7357-811420D14D6A    docker.io/alfuerst/hello-iluvatar-action:latest    io.containerd.runc.v2

  when two container with same name are registered 
    the last one is used to actually run the function 
```



### What is the summary of what I just read? (read)

  the name of the function can be obtained from containerd-shim-runc-v2 command line 

  actual threads to be scheduled are the children of this process

### What are the fundamentals? (reason)
  
  control groups are just extra restrictions on a regular process 

  from linux perspective it's a process tree at the end of the way 

### What level of understanding do I have?
  
  rudimentary  


## Overall Design 

### Flow of events 

```
       Kernel                   │              BPF Program                     │       User Space
────────────────────────────────┼──────────────────────────────────────────────┼───────────────────────────────────────
                                │ (called on each core)                        │
    task started ───────────────┼─► select_cpu ────────────► enqueued called─► │
                                │      ▲                                     │ │
                                │      │                                     │ │
                                │      │                                     │ │
        ┌───────────────────────┼──────┘                                     └─┼────► dispatch called
        │                       │                                              │             │
        │                       │                                              │             │
        │                       │                                              │             ▼
        │                       │                         ┌───dispatch called ◄├───── properties updated
     slice expires              │                         │                    │
        ▲                       │                         │                    │
        │                       │                         │                    │
        │                       │                         │                    │
        └─────────  run queue ◄─┼─────────────local dsq ◄─┘                    │
                                │                                              │
                                │                                              │
                                │                                              │
```


## Designing 

### CSV Reading 

What are we designing for? 
  
  scheduler that can schedule container functions 

#### Components 

  * scheduler 
  * csv of function characteristics  
  * pids of functions

#### Flows 

  reads the csv of characteristics 

  whenever requests for scheduling a task comes in 

    checks if pid corresponds to a function
    if so 
      uses the characteristics to schedule the function

CSV function characteristics - # sort of a slowly changing thing  
  function name
  e2e time 
  preferred core 

CSV of pids - # very dynamic thing 
  function_name: array of pids  
  
```
   # Characteristics CSV 

      func_name,e2e_time,preferred_core
      hello-0.1-08F7F119-9BEE-D709-4DD9-89BA20C70254,0.1,1
      hello-0.1-08F7F119-9BEE-D709-4DD9-89BA20C70254,0.1,2
      hello-0.1-1BCB4DAB-0DF4-5549-D5A4-973D6D7F7249,0.1,3
      stress-ng-matri,0.1,12

   # PIDs CSV 

      pid,func_name
      3012,"stress-ng-matri"
      19020,"hello-0.2-DF1ADCCF-84F9-B80E-3C26-FDE0833B7082"
      19041,"hello-0.2-DF1ADCCF-84F9-B80E-3C26-FDE0833B7082"
      19114,"hello-0.2-DF1ADCCF-84F9-B80E-3C26-FDE0833B7082"
      18938,"hello-0.1-08F7F119-9BEE-D709-4DD9-89BA20C70254"
      18958,"hello-0.1-08F7F119-9BEE-D709-4DD9-89BA20C70254"
      19011,"hello-0.1-08F7F119-9BEE-D709-4DD9-89BA20C70254"
```

### Running / Stopping Eviction

#### What are we designing for? 
  
  To ensure, pids that belong function cgroups are scheduled exactly the way 
  we want them to be. 

  Why?
    to ensure that effect from csv is trickeled down to the actual running tasks 

#### Components 
  
  user - single thread process  

  bpf - per cpu callbacks to make scheduling decisions 
    link list for pids

  user-bpf 
    ring buffer to push pids to bpf

#### Flows 

  csv reading 
    as read - push each pid for eviction 

  user -> ringbuffer 
    pid to evict 

  ringbuffer -> bpf 
    fetch and push to a link list 

  bpf 
    stop call back 
      parse the link list to check pids to evict 
      if a match - push to dispatch ring buffer 
      remove from the link list 


### Questions 

  Do we need to maintain all the pids that should be evicted in bpf? 
    
    no it should be command / act mechanisim 
  
  Does reading of csvs trigger page faults? 
    
    What happens in the case the csv files grow? 


### Selective CPU Policy 
  
  A policy that
    * fixes the cpu based on preferred core in csv 
      * enforced in following callbacks 
        * select_cpu
        * dispatch

#### Flow of Events 
  
  policy periodically reads the csv 

  communicates to the bpf program the pids which must be excluded from optimizations  
    
    whenever csv is read 
      it writes the whole list of pids to inactive array 
      after the write is complete 
      atomically switch the active array 

  whenever dispatch is called 
    update the stuff about the task
      currently it's only setting the CPU of the task

#### Components 
  
  communication channel to indicate the pids to exclude
    
    two arrays 

      one active 
      one inactive

    index to the active array

#### Questions 

  Why do we need two arrays? 

    the bpf select is called on each core separately 
    so the array must not change in the middle of the call

    reading is done in user space in a single thread 
 

  Why do we need to atomically switch? 

    so that no race condition occurs




## Investigations   
```
            ┌─────────────────────────────┐
            │                             ▼
   Capture Observations              Ask Questions
            ▲                             │
            └─────────────────────────────┘

                Until Root Cause is Found
```
### CPU not changing after going through dispatch call
  
  Observation 0

    * task goes to dispatch call 
      * where local dsq should be changed 
    * without explicitly pushing to dispatch from stop 
      * task doesn't go through the rustland_enqueue callback which actually submits to the dispatch ring buffer 
      
  Questions 0 

    * Is local dsq being changed?  
    * Can I print out local dsq from bpf code? 
    * Why is enqueued callback not called?  
      * ? 
      *
    * Is p->scx.dsq set for this task? 
      * no! 
      * [mydebugs]: pid=1441256 (gunicorn) dsq=0000000000000000
    * Does bpf try to assign it a local dsq? 
      * yes it does 
      *
  Observation 1 

    * Dispatch call happens with dsq_id=2 - that is local dsq and with specified dsq=10
    
          [mydebugs] dispatch: dsq_id=2 pid=1441256
          [mydebugs] dispatch: dsq_id=10 pid=1441256

  Questions 1 
    
    * Where is local dispatch coming from? 
      * rustland_select_cpu
     
  Observation x 
  Questions x 

#### What is the root cause? 
  
  select cpu callback was directly pushing the task to local dsq  
  
  after using the epid mechanisim to skip that it works fine 

## Description











