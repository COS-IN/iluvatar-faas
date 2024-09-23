# Stats outputed by the policy 

## Why is this feature required? 
  
  To get an Observability into what's happening in the policy. 

### Configuration
  
  a metric to be outputed 

### Mechanisms
  
  capture metrics in the bpf scheduler as it's running 
    in a structure 
  
  have the userspace thread read that structure on each run and output it in a 
  proper format 

### Design (Words)
  
  comp
### Design (ASCII Flow Diagram)

## Implementation Details 
### Components 

  struct stats{
    tsks_Q[ num_of_Qs ],
    #of cgroups, 

  }

### Flows 
### Verification  
## Questions

    * #tasks in a Q  
    * #tasks in a bucket  
 
