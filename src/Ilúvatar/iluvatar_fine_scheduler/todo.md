# Fine Grain Scheduling 


<!-- vim-markdown-toc Marked -->

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
* [Designing](#designing)

<!-- vim-markdown-toc -->

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

  * implement specific functions on specific core policy 
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
  
  * Understand scx_layered (it handles control groups)
    * [x] read main.rs code  
    * read bpf.c code 

  * solve the function threads not moving issue 
    * understand 
      * Does control group threads go through the dispatch call? 
        * no! - verified using printpid in dispatch callback 
      * Why not? 
        * dispatch is only called when there are no tasks to run in the local dsq and global dsq 
      * Does this task exist in the local dsq? 
        * ?     
      * How does layered scheduler handle control group tasks? 
        * ? 

  * add new callback to the fifo scheduler  
    * [x] see if I can just use the existing scheduler 
      * [x] select cpu is already there in main.bpf.c 
    * [x] Can I evict local dsqs? 
      * [x] I couldn't find any such api in kernel/sched/ext.c  
    * add main.bpf.c to the scheduler 
      * update it to print pid and task name of all the dispatched tasks 

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


  Thu 16 May 2024 10:46:56 AM EDT
    * 

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


## Designing 

What are we designing for? 
  
  scheduler that can schedule container functions 

Components: 

  * scheduler 
  * csv of function characteristics  
  * pids of functions

Scheduler 

  reads the json of characteristics 

  whenever requests for scheduling a task comes in 

    checks if pid corresponds to a function
    if so 
      uses the characteristics to schedule the function

CSV function characteristics - # sort of a slowly changing thing  
  function name
  e2e time 
  preferred core 

JSON of pids - # very dynamic thing 
  function_name: array of pids  

  
```
   # CSV 

   func_name                                      , e2e_time , preferred_core
   hello-0.2-B0A185DE-E2D8-D928-8DC4-02BB316EF728 , 0.1      , 1
  
   # JSON 

   {"hello-0.2-B0A185DE-E2D8-D928-8DC4-02BB316EF728": [343157, 343158, 343159]}

```












