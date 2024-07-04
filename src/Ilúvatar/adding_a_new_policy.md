# Adding a new policy 

Copy the policy from rust scheduler from scx repo 
for commit-id 973aded5a893eb6d7a3a9af9e45aeb8ce59db3f9.

Update it's toml file to
  * reflect proper name 
  * use scx_utils, scx_rustland and ipc-channels crates

Modify  

  * worker to 
    * launch the binary and establish channel with it 
    
  * scheduler to
    * establish ipc channel with the worker 
    * switch the pids received on the channel to SCHED_EXT class 



## LAVD Modification 
  
### Next Actions 

  * [x] update worker to launch the policy  
  * [x] update ansible worker.yml playbook to copy the policy binary 
  * check error logs that it crashes 
  * update the scheduler to establish the channel 
  * receive pids on it 
  * switch them to SCHED_EXT class
  




