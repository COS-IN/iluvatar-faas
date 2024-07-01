# Collecting schedstats 

## Why do we want to collect schedstats? 
  
  To get a review of how well a function is performing under a specific scheduling policy. 

## What's the best way to collect schedstats? 
  
  * create a script to capture the schedstats for a given pid  
    * outputing to stdout 
  
    * shouldn't it be in rust? 
      * yes would be easier to integrate with the rest of the codebase
        * Has anyone else done it? 
          * proc_sys_parser crate is there but doesn't work for given pid 
  



