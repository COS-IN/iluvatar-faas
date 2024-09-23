# Verification of the Task Size Interval Assignment Policy 

## Functional Verification 

### Testcase results 

  get_groupid
```
           <...>-3488710 [008] ...11 1448208.866190: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 0 -> gid 0 -- should be 0 -- passed: 1
           <...>-3488710 [008] ...11 1448208.866191: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 100 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866192: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 1000 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866193: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 2000 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866194: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 3000 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866195: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 4000 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866196: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 5000 -> gid 3 -- should be 3 -- passed: 1
```

  gen_qid_new
```
 fs_policy_tsksz-3488710 [008] ...11 1448208.866197: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: -1 -> qid -1 -- should be -1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866198: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866199: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866199: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866200: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866201: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 4 -- should be 4 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866202: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 5 -- should be 5 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866203: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866203: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 6 -- should be 6 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866204: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 7 -- should be 7 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866205: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 8 -- should be 8 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866206: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 9 -- should be 9 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866207: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 10 -- should be 10 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866208: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 11 -- should be 11 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866209: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 6 -- should be 6 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866209: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 12 -- should be 12 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866210: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 13 -- should be 13 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866211: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 14 -- should be 14 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866212: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 15 -- should be 15 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866213: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 16 -- should be 16 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866214: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 17 -- should be 17 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866215: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 12 -- should be 12 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866216: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 18 -- should be 18 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866217: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 19 -- should be 19 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866218: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 20 -- should be 20 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866219: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 21 -- should be 21 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866220: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 22 -- should be 22 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866221: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 23 -- should be 23 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866222: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 18 -- should be 18 -- passed: 1
```

  qid_to_groupid
```
 fs_policy_tsksz-3488710 [008] ...11 1448208.866223: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: -1 -> gid -1 -- should be -1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866224: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 0 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866226: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 1 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866229: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 2 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866230: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 3 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866231: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 4 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866232: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 5 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866233: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 0 -> gid 0 -- should be 0 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866234: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 6 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866236: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 7 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866237: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 8 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866237: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 9 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866238: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 10 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866239: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 11 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866240: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 6 -> gid 1 -- should be 1 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866259: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 12 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866260: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 13 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866261: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 14 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866261: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 15 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866262: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 16 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866263: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 17 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866264: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 12 -> gid 2 -- should be 2 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866265: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 18 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866266: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 19 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866267: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 20 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866268: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 21 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866269: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 22 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866270: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 23 -> gid 3 -- should be 3 -- passed: 1
 fs_policy_tsksz-3488710 [008] ...11 1448208.866271: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 18 -> gid 3 -- should be 3 -- passed: 1
```

### Capturing Cgroups of Functions 

##### chashmap insertion 

  Steps 
  * register a single func 
  * invoke 

  Expected outcome
  * 'cat /sys/kernel/debug/tracing/trace_pipe | grep -i "\[chashmap\]"'
  * should show inserting, found logs  
  
  logs
```
   root@v-021:/data2/ar/workspace# cat /sys/kernel/debug/tracing/trace_pipe | grep -i "\[chashmap\]"
        gunicorn-3009925 [023] ...11 1783745.330394: bpf_trace_printk: [info-tsksz] [chashmap] inserting cgroup 49391 - 9c12eb2cbf11f4 with Q 1
        gunicorn-3009925 [023] ...11 1783745.330396: bpf_trace_printk: [info-tsksz] [chashmap][init][____tsksz_init_task] inserting cgroup: 49391 - 9c12eb2cbf11f4 because of task 3010021 - gunicorn
        gunicorn-3010021 [001] ...11 1783745.493700: bpf_trace_printk: [info-tsksz] [chashmap][____tsksz_init_task] found cgroup: 49391 - 9c12eb2cbf11f4 for task 3010039 - gunicorn
        gunicorn-3010021 [001] ...11 1783745.626219: bpf_trace_printk: [info-tsksz] [chashmap][____tsksz_init_task] found cgroup: 49391 - 9c12eb2cbf11f4 for task 3010046 - gunicorn

        gunicorn-3010076 [023] ...11 1783746.193474: bpf_trace_printk: [info-tsksz] [chashmap] inserting cgroup 49413 - worker-health- with Q 2
        gunicorn-3010076 [023] ...11 1783746.193475: bpf_trace_printk: [info-tsksz] [chashmap][init][____tsksz_init_task] inserting cgroup: 49413 - worker-health- because of task 3010129 - gunicorn
```

##### qid assignment update  

  Steps 
  * register a single func 
  * do a multi invoke 

  Expected outcome
  * 'cat /sys/kernel/debug/tracing/trace_pipe | grep -i qid_assignment'
  * should show "now is assigned logs"  
  
  logs
```
```

##### pidstat   

  Steps 
  * register a single func 
  * do a multi invoke 

  Expected outcome
  * cpu of the task group and all children should change as expected in previous results 

  logs 
```
```


