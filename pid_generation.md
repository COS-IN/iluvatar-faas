
# Designing: Pid csv generation 

## What are we designing for? 

  Generating a csv of pids corresponding to a specific fqdn.  

  Why?
    
    it would be used by the scheduler to pick characteristics to base the decicions on 

## Components 

  What existing code is there? 

    send_bg_packet is called from run_container
    which sends a packet on channel to be receieved by the background thread 
    which launches pgprep to determine the parent pid and spit it out to logs 

  How can i incorporate the new requirement? 
  
    * [x] maintain a map of pid to fqdn 
    * [x] add to this map if doesn't exist already in this background thread 
    * [x] write updated map to csv file 
    * use ps tree command to get all the pids 
      * Should we also capture threads? 
        * yes! - they have their own pids and therefore should be scheduled accordingly 
  
  PID map 
    Where? 


## Description





