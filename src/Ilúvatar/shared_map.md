
# Sharing map between control plane and scheduler 


## Control Plane 

  * creates the map before scheduler is launched 
    * bpf.c with the map                 ✓
    * integration of libbpf_rs into the control plane                 ✓
    * launch the bpf program and pin the map to fs at predefined location 

## Scheduler 

  * use hardcoded path for the map location   
  * spit out in the init_task callback 
  * cleanup the code 
  * create tasksize interval assignment policy 
 


