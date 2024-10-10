


# Load Balancing 

  * Update the update_qid func to change qid assignment if the Q has more then 60 tasks in it 
    * it should remove the load from Q 0 in the mixed trace exp 




# Adding Task Context 

## Why is this feature required? 
  
  * to capture metrics on per task level 
    * actually remember the Qid it is assigned to 
  * use it to add or sub from per Q utilization context  
  
### Configuration

  none 

### Mechanisms
  
  task_hashmap 
    key: pid 
    context: {
      qid: <s32>
    }

  select_cpu(p)
    update_qid( p )
      task_hashmap[pid].qid = update as fetched from cgroup context  

  enqueue(p)
    task_hashmap[pid].qid = update as fetched from cgroup context  

  init_task(p)
    task_hashmap[pid].qid = assigned qid 

  exit_task(p)
    use task_hashmap[pid].qid to remove from Q context 

## Implementation  

  * hashmap                                       ✓
  * updates in callbacks                          ✓
  * print out qid assignment at all the locations ✓

## Verification 
  
  * single func exp using gzip                 ✓
  * verify via traces what happens to the qid assignment                 ✓

# Update Q task count tracking 

  * done via bpf_dsq_len helper 



