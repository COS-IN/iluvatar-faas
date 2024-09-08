
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

  ~ 0.5 hrs
    * qid_to_groupid( qid )                            ✓
      * reverse lookup

  * update the cgroup hashmap to have oqid and nqid                 ✓

  * verify that cgroup id captured in init_task and those pushed into func_chars bpf_map are same
    * no it's incorrect! 

  * fix the cgroup-id match issue                 ✓
    * possible ways to fix it 
      * convert name to digit in bpf program and then we would be good 
        * use bpf_strtoul to conver the name to  
```
  oup-id][chashmap_insert] -- 8317 -OK-inserted- a9cf888125b0829, qid: 0
  oup-id] converted key: 764759300053338153
  oup-id][map][func_characs] key: 764759300053338153 e2e: 0
  oup-id][map][func_characs] key: 764759300053338153 e2e: 0
  oup-id][map][func_characs] key: 764759300053338153 e2e: 299
  oup-id][map][func_characs] key: 764759300053338153 e2e: 299
```
 
  * update select_cpu to 
    * update the nqid allocation of the task p based on e2e 

      * update_nqid( p )
        * e2e to groupid 
        * nqid to groupid 
        * if they are same - nothing todo
        * otherwise:
          * oqid = nqid 
          * nqid = gen_qid( gid )

  * remember - task context 
    * put qid in it 
    * use it to keep track of if all the tasks that have transitioned 
    * similarly build a Q context to keep track of the Q state 

### Flows 

  
  * update dispatch to 
    * each per CPU SharedQ is consumed regardless of anything 
      * consume( oqid )              ✗
      * consume( nqid )              ✗

### Verification  

  * run a trace with two functions 
  * capture perfetto trace 
  * verify that two functions are constrained to their respective Q groups 



