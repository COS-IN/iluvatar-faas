# Constraining User Space Policy 

## What are we designing? 
  
  A user space scheduling policy that constrains cgroup tasks 
  to a group of cores. 

  Why?
    
    To verify if certain functions benefit from locality or not. 

## Questions (Reasoning) 
 
### How does the cgroup callback behave? 
    
  build and run the pair policy  
  verify through the logs what cgroup information is available  

  Does it trigger when function is registered? 

  Do we have the name of the function? 

  Is it triggered even if the class of the task is not sched_ext? 

    In this case, how to change the class of the tasks to sched_ext for those belonging to this cgroup? 

### How would the policy pick which cpu to run the task on? 
  
  use the pick any function                ✓ 
    it returns an idle cpu if available  
    otherwise it would just pick any 

  use the pair policy mechanics  
    maintain the active bitmask 
    find idle cpu 
    ping that cpu to schedule the task 

### What if the cpu is busy? 

  all tasks would be placed into a global dsq

## Possible ways to implement  

### Strip down the pair policy                ✗ 

  seems like a good approach 

### Write from scratch                        ✓ 

  will give me a very good understanding of what is what 
  
## Design

  cgroup creation    
    tasks belonging to the cgroup have their sched class switched to sched_ext  
      (already in place via channels and syscall)

  execution of the tasks 
    
    tasks are placed into a global dsq 
      enqueue callback 

    tasks are picked from the dsq 
      select_cpu and running callback  
      executed on available free cpus from a fixed set  

## Next Actions 

  * pick stuff from the pair policy   
    * start writing a bare bones scheduling policy in rust - build and getting trace info 
    * answer questions regarding the cgroup callback behavior
    * implement the skeleton design 





