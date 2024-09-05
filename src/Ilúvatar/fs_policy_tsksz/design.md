
# FS policy: Task Size Interval Assignment  

## Why is this feature required? 

  * first basic policy driven by metadata 
  * to show case that 
    * metadata pipeline works okay 
    * it can be used to derive the bpf scheduler 

### Configuration

  * threshold for Q groups - e2e 
  * number of group buckets 

### Mechanisms

  * e2e to group id 
  * group id to Q id 
  * transitioning tasks from old Q to a new Q 
  * check and update Q ids 

### Design (Words)

  * it's easier and faster to draw a picture on paper instead! 

### Design (ASCII Flow Diagram)

  * on paper                 ✓

## Implementation Details 

  **Test Driven Development**
    write test case for the function 
    implement the function 
    verify it works as expected 
    move on to next function 

  start with independent entitities 

### Components 

  
  ~ 1.3 hrs
    * get_groupid( e2e )                               ✓
      * based on the e2e thresholds - generate groupid
    * resolved compilation and running issues as well.

  ~ 1.3 hrs
    * gen_qid() --> gen_qid( gid )                     ✓
      * counters for each gid
      * generate qid based off that

  * qid_to_groupid( qid ) 
    * reverse lookup 

  * update the cgroup hashmap to have oqid and nqid 
    
  * update_nqid( p )
    * e2e to groupid 
    * nqid to groupid 
    * if they are same - nothing todo
    * otherwise:
      * oqid = nqid 
      * nqid = gen_qid( gid )

### Flows 

  * update enqueue to 
    * put task p into nqid 
    
  * update dispatch to 
    * consume( oqid )
    * consume( nqid )
    * check all Qs if they are empty 
      * and kick the target cpu if it's idle 

### Verification  

  * run a trace with two functions 
  * capture perfetto trace 
  * verify that two functions are constrained to their respective Q groups 



