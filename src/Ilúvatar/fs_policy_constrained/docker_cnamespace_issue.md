# Docker namespace issue 

## What is the desired behavior? 
  
  * cgroup->krn->name should be the same as with containerd backend 
  
## What is preventing it? 

  * it's the way docker calls the containerd apis 
    * it assigns a unique guid of it's own to the cgroup name 

## What would solve it? 

  * changing docker behavior to assign the name we give 
  * finding another way to map - tasks to cgroup ids of the functions 
    * tasks to cgroup ids 

## Can each of the solutions be implemented?   
  
  * not possible to change docker behavior - since no such arg is exposed for run command 
    * and it's usually that such systems do this to maintain an abstraction and avoid poluting the namespace with common names 
 
  * from tasks to cgroup ids  
    * if the task name is guicorn 
      * put it on ring buffer  
      * save the cgroup id 
    * from userspace to match metadata 
      * use cgroup unique id instead of the function name 


