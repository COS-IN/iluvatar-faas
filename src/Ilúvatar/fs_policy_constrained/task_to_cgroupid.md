
# Task to Cgroup ID - Sol to avoid docker namespace problem 

## Critical Assumptions 
  
  * all tasks named gunicorn in the system belong to some function only  

## Questions (Reasoning)
  
  Does task name have guincorn in it? 
    ? 
  
  At what point should the task pid be put on ring buffer?  
    init_task 
  
  Can we get cgroup ids from the userspace for docker containers? 
  

## Next Actions 

  * check task name from bpf scheduler                ✓
    * yes gunicorn task name is there 
  * check cgroup ids for docker containers - how they can be retrieved in user space ( for future policies - when metadata would need to be matched )                ✓
    * container id in "sudo docker ps" command is the cgroup name 
    
  * implement 
    * task name matching and putting on ring buf 



