# Fixed Locality Policy  


## What are we designing for? 

  A policy that would force locality for specific functions. 

  Why?
    To test if enforcing locality helps. 

  Why?
    To see if we can improve the performance of short running functions using sched_ext.


## Components 

### BPF Program 
  
  * has a pid to cpu map 
  * it should be enforced in select_cpu call 

### User Space Process 
  
  * statically initialize the function to cpu map in the scheduler  
  * for every pid that comes in 
    * find corresponding function name 
    * check if the function exists in the cpu map 
    * update the pid to cpu map in the bpf program
    * set the cpu to the one in the cpu map while dispatching 

## Flow of events 


## Description


## Questions ( Reasoning )

  How would cpu map be constructed? 
    
    we can pass the static map to the scheduler as csv 
  
  Why do I have to check the pids in the user space process? 
    
    Can I push the characteristics map and the pid map to the bpf program?


## New Design for the Policy 

  * user space process get's kicked periodically 
  * it reads the csvs and pushes them to the bpf program 
  * the bpf program implements the main logic of the policy that is enforcing locality

### Questions 
  
  How would dispatch work in this case? 

  
  Can we request to switch the scheduling class of specific pids to sched_ext in the scheduler?
    
    it should be possible with sched_setscheduler api 
    
    How does sched_ext switch all the tasks to sched_ext? 
      
      unable to figure out at the moment  

      Is there a way to filter out or switch specific pids through the ext api? 
        don't know! 













