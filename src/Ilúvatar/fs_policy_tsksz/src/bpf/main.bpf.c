/* Copyright (c) Abdul Rehman <abdulrehmanee010@gmail.com> */
/* 
   A constraining bpf scheduler that runs tasks in a fifo manner 
   on a given set of cores only. 

   Tasks run for a fixed timeslice.

 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */
#include <scx/common.bpf.h>
#include <scx/ravg_impl.bpf.h>
#include "intf.h"

#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

char _license[] SEC("license") = "GPL";

UEI_DEFINE(uei);

/* 
   Important references from Linux Kernel 

     struct kernfs_node
       /data2/ar/finescheduling/sched_ext/sched_ext-sched_ext/debian/linux-headers-6.9.0/usr/src/linux-headers-6.9.0/include/linux/kernfs.h:203

     struct cgroup
       linux-headers-6.9.0/include/linux/cgroup-defs.h

*/


////////////////////////////////
// Macros 

#define FETCH_KERNEL_STR(x, name)                             \
	char name[MAX_NAME_LEN];                                  \
	int n = bpf_probe_read_kernel_str(name, MAX_NAME_LEN, x); \
	n = n > MAX_NAME_LEN ? MAX_NAME_LEN : n;

#define MAX_FUNCS 50 
#define KEYSIZE 15 // because the kernel fs inode name is 15 characters 

// maximum number of tasks that can be handled
#define MAX_ENQUEUED_TASKS 8192

// it is filled in during init from the tsksz core array filled by
// userland
#define CMASK_GLOBAL_KEY 0x0

/*
   48 / 2 -> 24 
   i -- [0, 24) 
   i -- 23 
   cpu 23*2 -- 46  
   cpu 46 
   cpu 47 
*/
#define SHARED_DSQ 24 
#define USCHED_DSQ SHARED_DSQ + 1
#define USCHED_CORE MAX_CPUS - 1
#define MAX_CGROUPS 64

// info msg with a specific tag
#define info_msg(_fmt, ...)                                       \
	do {                                                      \
		bpf_printk("[info-tsksz] " _fmt, ##__VA_ARGS__); \
	} while (0)

// info msg with a specific tag
#define info_msg_fm_dispatch(_fmt, ...)                                       \
	do {                                                      \
		bpf_printk("[info-tsksz] " _fmt, ##__VA_ARGS__); \
	} while (0)
// info msg with a specific tag
#define info_msg_fm_dispatch(_fmt, ...)

// warn msg with a specific tag
#define warn_msg(_fmt, ...)                                       \
	do {                                                      \
		bpf_printk("[warn-tsksz] " _fmt, ##__VA_ARGS__); \
	} while (0)

#define callback_msg(_fmt, ...)                                          \
	do {                                                             \
		s32 cpu = scx_bpf_task_cpu(p);                           \
		info_msg(_fmt " cpu: %d -- %d - %s", ##__VA_ARGS__, cpu, \
			 p->pid, p->comm);                               \
	} while (0)

// Maximum length of name (struct kernfs_node -> name) - comm has 16 length in
// kernel 
#define MAX_NAME_LEN 16

// [0,2000) [2000,4000) [4000,...)
// there are 2 buckets in this example, third is default for larger functions 
// the reserved bucket 0 for funcs that aren't yet categorized 
#define MAX_E2E_BUCKETS 4
#define RESERVED_E2E_BUCKET 0


////////////////////////////////////////
// Structures 

typedef struct CharVal{
    u32 prio;
    u32 e2e;
    u32 loc;
} CharVal_t;

// let's create a hashmap 
// a hash map 
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FUNCS);
  __uint(key_size, sizeof(char)*KEYSIZE);         /* cgrp ID */
  __uint(value_size, sizeof(CharVal_t)); /* Value Structure */
} func_characs SEC(".maps");


/*
  hashmap for capturing cgroups along with properties 
*/
typedef struct cgroupvalue {
	s32 qid;
	char name[MAX_NAME_LEN];
} Cgroupvalue;

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, MAX_CGROUPS);
	__type(key, __u64);
	__type(value, Cgroupvalue);
} CgroupsHashMap SEC(".maps");


/*
 * Heartbeat timer used to periodically trigger the check to run the user-space
 * scheduler.
 *
 * Without this timer we may starve the scheduler if the system is completely
 * idle and hit the watchdog that would auto-kill this scheduler.
 */
struct usersched_timer {
	struct bpf_timer timer;
};

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1);
	__type(key, u32);
	__type(value, struct usersched_timer);
} usersched_timer SEC(".maps");

// The map containing pids of tasks that are to be switched to SchedEXT policy.
// it is drained by the user space thread
struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, MAX_ENQUEUED_TASKS);
} queued_pids SEC(".maps");


/////////////////////////////////////////////
// Global Variables 

/*
 * Scheduler attributes and statistics.
 */
u32 usersched_pid; /* User-space scheduler PID */
const volatile u64 slice_ns = SCX_SLICE_DFL; /* Base time slice duration */

/*
 * Effective time slice: allow the scheduler to override the default time slice
 * (slice_ns) if this one is set.
 */
const volatile u64 effective_slice_ns = 1 * MSEC_PER_SEC;

// Number of tasks being handled by the bpf scheduler
volatile u64 nr_tasks = 0;
volatile u64 nr_eq_tasks = 0;


volatile u32 e2e_thresholds[MAX_E2E_BUCKETS -2]; // we don't need thresholds
                                                 // for reserved and rest
                                                 // buckets

// Q related 
static volatile s32 bkt_next_qid[MAX_E2E_BUCKETS] = {0};

// we can later change this assignment - so that one Q may have more cpus
// there is a bug in the bpf verifier - which isn't allowing to use following
// to set the array, hence to avoid it capacity is twice the number of cpus 
//        cpu = i * 2 + 1;
static s32 cpu_to_qid_array[MAX_CPUS*2];

/*
 * Flag used to wake-up the user-space scheduler.
 */
static volatile u32 usersched_needed;

/////////////////////////////////
// Function Declarations 

static __always_inline struct cgroup * task_to_cgroup(struct task_struct *p);
static __always_inline CharVal_t * func_characs_present( char *key_name );
static __always_inline Cgroupvalue *chashmap_present(__u64 cid);


/////////////////////////////////
// Function Definitions 

s32 get_groupid( u32 e2e ) {
    int i;

    // can setup if else branch for thresholds 
    if( e2e == 0 ){
        return -1;
    };
    if( e2e < e2e_thresholds[0] ){
        return 1;
    }else if ( e2e < e2e_thresholds[1] ){
        return 2;
    }
    return 3;
}

/*
   Test Results 
      root@v-021:/data2/ar/workspace/temp# cat /sys/kernel/debug/tracing/trace_pipe | grep -i test
      fs_policy_tsksz-2481710 [002] ...11 227122.741565: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 0 -> gid -1 -- should be -1 -- passed: 1
      fs_policy_tsksz-2481710 [002] ...11 227122.741567: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 100 -> gid 1 -- should be 1 -- passed: 1
      fs_policy_tsksz-2481710 [002] ...11 227122.741568: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 1000 -> gid 1 -- should be 1 -- passed: 1
      fs_policy_tsksz-2481710 [002] ...11 227122.741569: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 2000 -> gid 2 -- should be 2 -- passed: 1
      fs_policy_tsksz-2481710 [002] ...11 227122.741570: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 3000 -> gid 2 -- should be 2 -- passed: 1
      fs_policy_tsksz-2481710 [002] ...11 227122.741571: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 4000 -> gid 3 -- should be 3 -- passed: 1
      fs_policy_tsksz-2481710 [002] ...11 227122.741572: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 5000 -> gid 3 -- should be 3 -- passed: 1
*/
static __always_inline void verify_get_groupid(){
    s32 gid; 
    s32 sgid; 

#define TESTCASE_get_groupid( e2e, sgid ) \
    gid = get_groupid( e2e ); \
    info_msg("[test][get_groupid] e2e: %d -> gid %d -- should be %d -- passed: %d ", \
                e2e, \
                gid, \
                sgid, \
                (gid == sgid) \
             );
    
    TESTCASE_get_groupid( 0, -1 )
    TESTCASE_get_groupid( 100, 1 )
    TESTCASE_get_groupid( 1000, 1 )
    TESTCASE_get_groupid( 2000, 2 )
    TESTCASE_get_groupid( 3000, 2 )
    TESTCASE_get_groupid( 4000, 3 )
    TESTCASE_get_groupid( 5000, 3 )
}

static __always_inline s32 gen_qid_new( s32 gid )
{
    if( !(0 <= gid && gid < MAX_E2E_BUCKETS) ){
      return -1;
    }

	s32 t = bkt_next_qid[gid]++;
    s32 gap = SHARED_DSQ / MAX_E2E_BUCKETS; // 6 
    s32 lower = gap * gid; // 0,6
    s32 upper = lower + gap; // 6,12 

	if ( bkt_next_qid[gid] == upper ) {
		bkt_next_qid[gid] = lower;
	}

	return t;
}

/*
   Test Results 
    root@v-021:/data2/ar/workspace/temp# cat /sys/kernel/debug/tracing/trace_pipe | grep -i gen_qid_new
     fs_policy_tsksz-2491375 [047] ...11 231851.589544: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: -1 -> qid -1 -- should be -1 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589545: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589546: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589547: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589547: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589548: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 4 -- should be 4 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589549: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 5 -- should be 5 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589550: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 0 -> qid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589551: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 6 -- should be 6 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589552: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 7 -- should be 7 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589552: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 8 -- should be 8 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589553: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 9 -- should be 9 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589554: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 10 -- should be 10 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589555: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 11 -- should be 11 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589556: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 1 -> qid 6 -- should be 6 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589557: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 12 -- should be 12 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589558: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 13 -- should be 13 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589558: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 14 -- should be 14 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589559: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 15 -- should be 15 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589560: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 16 -- should be 16 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589561: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 17 -- should be 17 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589562: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 2 -> qid 12 -- should be 12 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589563: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 18 -- should be 18 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589563: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 19 -- should be 19 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589564: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 20 -- should be 20 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589565: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 21 -- should be 21 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589566: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 22 -- should be 22 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589567: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 23 -- should be 23 -- passed: 1
     fs_policy_tsksz-2491375 [047] ...11 231851.589568: bpf_trace_printk: [info-tsksz] [test][gen_qid_new] gid: 3 -> qid 18 -- should be 18 -- passed: 1
*/
static __always_inline void verify_gen_qid_new(){
    s32 qid; 
    s32 sgid; 

#define TESTCASE_gen_qid_new( gid, sqid ) \
    qid = gen_qid_new( gid ); \
    info_msg("[test][gen_qid_new] gid: %d -> qid %d -- should be %d -- passed: %d ", \
                gid, \
                qid, \
                sqid, \
                (qid == sqid) \
             );
    // for config max dsqs 24 and - max buckets 4  
    // 0 -> [0,6)
    // 1 -> [6,12)
    // 2 -> [12,18)
    // 3 -> [18,24)

#define TESTCASES_gen_qid_new( gid, sqid ) \
    TESTCASE_gen_qid_new( gid, sqid + 0 ) \
    TESTCASE_gen_qid_new( gid, sqid + 1 ) \
    TESTCASE_gen_qid_new( gid, sqid + 2 ) \
    TESTCASE_gen_qid_new( gid, sqid + 3 ) \
    TESTCASE_gen_qid_new( gid, sqid + 4 ) \
    TESTCASE_gen_qid_new( gid, sqid + 5 ) \
    TESTCASE_gen_qid_new( gid, sqid + 0 )

  TESTCASE_gen_qid_new( -1, -1 ) 
  TESTCASES_gen_qid_new( 0, 0 )
  TESTCASES_gen_qid_new( 1, 6 )
  TESTCASES_gen_qid_new( 2, 12 )
  TESTCASES_gen_qid_new( 3, 18 )
}


static __always_inline s32 qid_to_groupid( s32 qid ){
    s32 gid = 0;

    if( !(0 <= qid && qid < SHARED_DSQ) ){
        return -1;
    }

    s32 gap = SHARED_DSQ / MAX_E2E_BUCKETS; // 6 
    s32 lower;
    s32 upper;

    bpf_for(gid, 0, MAX_E2E_BUCKETS){
      lower = gap * gid; // 0,6
      upper = lower + gap; // 6,12 

      if( lower <= qid && qid < upper ){
          break;
      }
    }
    if ( gid == MAX_E2E_BUCKETS ){
        return -1;
    }
    return gid;
}

/*
   Test Results: 
     root@v-021:/data2/ar/workspace/temp# cat /sys/kernel/debug/tracing/trace_pipe | grep -i qid_to_groupid
     fs_policy_tsksz-2497763 [026] ...11 235008.361905: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: -1 -> gid -1 -- should be -1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361906: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 0 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361907: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 1 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361908: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 2 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361909: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 3 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361910: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 4 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361911: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 5 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361912: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 0 -> gid 0 -- should be 0 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361912: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 6 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361913: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 7 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361914: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 8 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361915: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 9 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361916: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 10 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361917: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 11 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361918: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 6 -> gid 1 -- should be 1 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361919: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 12 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361920: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 13 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361921: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 14 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361922: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 15 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361923: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 16 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361924: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 17 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361925: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 12 -> gid 2 -- should be 2 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361926: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 18 -> gid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361927: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 19 -> gid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361927: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 20 -> gid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361928: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 21 -> gid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361929: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 22 -> gid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361930: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 23 -> gid 3 -- should be 3 -- passed: 1
     fs_policy_tsksz-2497763 [026] ...11 235008.361931: bpf_trace_printk: [info-tsksz] [test][qid_to_groupid] qid: 18 -> gid 3 -- should be 3 -- passed: 1
*/
static __always_inline void verify_qid_to_groupid(){
    s32 gid; 

#define TESTCASE_qid_to_groupid( qid, sgid ) \
    gid = qid_to_groupid( qid ); \
    info_msg("[test][qid_to_groupid] qid: %d -> gid %d -- should be %d -- passed: %d ", \
                qid, \
                gid, \
                sgid, \
                (gid == sgid) \
             );

/* 
   [0,6)   -> 0 
   [6,12)  -> 1
   [12,18) -> 2
   [18,24) -> 3
*/
#define TESTCASES_qid_to_groupid( qid, sgid ) \
    TESTCASE_qid_to_groupid( qid + 0, sgid) \
    TESTCASE_qid_to_groupid( qid + 1, sgid) \
    TESTCASE_qid_to_groupid( qid + 2, sgid) \
    TESTCASE_qid_to_groupid( qid + 3, sgid) \
    TESTCASE_qid_to_groupid( qid + 4, sgid) \
    TESTCASE_qid_to_groupid( qid + 5, sgid) \
    TESTCASE_qid_to_groupid( qid + 0, sgid)

  TESTCASE_qid_to_groupid( -1, -1 ) 
  TESTCASES_qid_to_groupid( 0, 0 )
  TESTCASES_qid_to_groupid( 6, 1 )
  TESTCASES_qid_to_groupid( 12, 2 )
  TESTCASES_qid_to_groupid( 18, 3 )
}

static __always_inline void update_qid_as_per_e2e( struct task_struct *p ) {

    // fetch cgroup name for the given task 
    struct cgroup *cgrp = task_to_cgroup( p );
    if( cgrp ){
        FETCH_KERNEL_STR( cgrp->kn->name, cname )

        CharVal_t * fvalue = func_characs_present( cname );
        if( fvalue ){

            Cgroupvalue * cgvalue = chashmap_present( cgrp->kn->id );  
            if ( cgvalue ){

              s32 gid = get_groupid( fvalue->e2e );
              s32 ogid = qid_to_groupid( cgvalue->qid );

              if ( ogid != gid ){
                  s32 nqid = gen_qid_new( gid );
                  info_msg( "[update_qid] qid: %d -> nqid %d for cgroup: %s pid: %d", 
                           cgvalue->qid,         
                           nqid,
                           cname,
                           p->pid
                         );
                  cgvalue->qid = nqid;
              }

            }else{
              info_msg( "[warning][update_qid] no cgroup value in hashmap for cgroup: %s", cname );
            }
        }else{
            info_msg( "[warning][update_qid] no func_characs for cgroup: %s", cname );
        }
    }else{
        info_msg( "[warning][update_qid] no cgroup for %d - %s", p->pid, p->comm );
    }
}

// Callback for bpf_for_each_map_elem
// long (\*callback_fn)(struct bpf_map \*map, const void \*key, void \*value, void \*ctx);
// callback continues if return 0 
//          stops if return 1
static long func_characs_cb_print (void *map, const char *key, CharVal_t *val, void *data){
    info_msg("[cgroup-id][map][func_characs] key: %s e2e: %lu", 
             key,
             val->e2e
             ); 
    return 0;
}

static __always_inline CharVal_t * func_characs_present( char *key_name )
{
	CharVal_t *cvalue = bpf_map_lookup_elem(&func_characs, key_name);
	return cvalue;
}

static __always_inline bool verify_qid( s32 qid )
{
	if (0 <= qid && qid < SHARED_DSQ) {
		return true;
	}
	return false;
}

static __always_inline bool verify_cpu(s32 cpu)
{
	if (0 <= cpu && cpu < MAX_CPUS) {
		return true;
	}
	return false;
}

static __always_inline s32 cpu_to_qid(s32 cpu)
{
    if ( verify_cpu( cpu ) ){
      // lookup array would be O(1) sol and most flexible
      return cpu_to_qid_array[cpu];
    }
    return -1;
}

// Generate a bpf cpumask with cpus that belong to qid
static __always_inline struct bpf_cpumask *qid_to_cpumask(s32 qid)
{
	struct bpf_cpumask *mask;
	s32 cpu = qid * 2;

	mask = bpf_cpumask_create();
	if (!mask)
		return NULL;

	bpf_cpumask_set_cpu(cpu, mask);
	bpf_cpumask_set_cpu(cpu + 1, mask);
    return mask;
}

void queued_pids_push(int pid)
{
	int *p = bpf_ringbuf_reserve(&queued_pids, sizeof(packet_pid_t), 0);
	if (p) {
		packet_pid_t *ps = (packet_pid_t *)p;
		ps->pid = pid;
		bpf_ringbuf_submit(ps, 0);
		info_msg("pushed %d", pid);
	}
}

static void chashmap_insert(__u64 cid, char *name)
{
	Cgroupvalue *cvalue = bpf_map_lookup_elem(&CgroupsHashMap, &cid);

	if (cvalue) {
		__builtin_memcpy_inline(cvalue->name, name, MAX_NAME_LEN);
	} else {
		Cgroupvalue new_cvalue;
		__builtin_memcpy_inline(new_cvalue.name, name, MAX_NAME_LEN);
        
		new_cvalue.qid = gen_qid_new( 0 );
		bpf_map_update_elem(&CgroupsHashMap, &cid, &new_cvalue,
				    BPF_NOEXIST);
		info_msg("[cgroup-id][%s] -- %d -OK-inserted- %s, qid: %d", 
                 __func__,
                 cid,
                 name,
                 new_cvalue.qid
        );
	}
}

static __always_inline Cgroupvalue *chashmap_present(__u64 cid)
{
	Cgroupvalue *cvalue = bpf_map_lookup_elem(&CgroupsHashMap, &cid);
	return cvalue;
}

static __always_inline struct cgroup * task_to_cgroup(struct task_struct *p)
{
	struct task_group *tg = p->sched_task_group;
	if (tg && tg->css.cgroup) {
		return tg->css.cgroup;
	}
    return NULL;
}

static __always_inline s32 task_to_qid(struct task_struct *p)
{
    struct cgroup *cgrp = task_to_cgroup( p );
	if ( cgrp ) {
        Cgroupvalue * cvalue = chashmap_present(cgrp->kn->id);
		if (cvalue) {
            return cvalue->qid;
		}
	}
    return -1;
}

static __always_inline void print_task_cgroup_stats(struct task_struct *p)
{
	struct task_group *tg = p->sched_task_group;
	if (tg) {
      struct cfs_bandwidth *cbs = &tg->cfs_bandwidth;
      info_msg("[stats][%d][%s]  cbs %p,  sched_idle %d",
               p->pid, 
               p->comm, 
               cbs, 
               tg->idle 
      );
	}
}

static inline bool match_prefix(const char *prefix, const char *str,
				u32 max_len)
{
	int c;
	if (!prefix || !str) {
		return false;
	}

	if (max_len == 0) {
		return false;
	}

	if (max_len == 1) {
		if (prefix[0] == '\0')
			return false;
		if (str[0] == '\0')
			return false;
	}

	bpf_for(c, 0, max_len)
	{
		if (prefix[c] == '\0')
			return true;
		if (c == (max_len - 1)) {
			return true;
		}
		if (str[c] != prefix[c])
			return false;
	}
	return false;
}

static inline bool match_prefix_kernel_str(const char *prefix, const char *kstr)
{
	FETCH_KERNEL_STR(kstr, name)

	if (n >= 0 && match_prefix(prefix, name, n)) {
		return true;
	}
	return false;
}

static __always_inline int cus_strlen(const char *cs)
{
	int len = 0;
	while (cs != NULL && *cs != '\0') {
		cs++;
		len++;
	}
	return len;
}



/*
 * Return true if the target task @p is the user-space scheduler.
 */
static inline bool is_usersched_task(const struct task_struct *p)
{
	return p->pid == usersched_pid;
}

/*
 * Return true if the target task @p is a kernel thread.
 */
static inline bool is_kthread(const struct task_struct *p)
{
	return !!(p->flags & PF_KTHREAD);
}

/*
 * Set user-space scheduler wake-up flag (equivalent to an atomic release
 * operation).
 */
static void set_usersched_needed(void)
{
	__sync_fetch_and_or(&usersched_needed, 1);
}

/*
 * Check and clear user-space scheduler wake-up flag (equivalent to an atomic
 * acquire operation).
 */
static bool test_and_clear_usersched_needed(void)
{
	return __sync_fetch_and_and(&usersched_needed, 0) == 1;
}

/*
 * Dispatch a task to the Shared DSQ.
 * 
 * Wakeup the target CPU. 
 */
static void dispatch_task( struct task_struct *p, s32 cpu, u64 task_slice_ns, u64 enq_flags, u64 dsq_id )
{
	u64 slice = task_slice_ns ?
                  task_slice_ns :
                  effective_slice_ns;

	// we only dispatch to a single global dsq
	scx_bpf_dispatch(p, dsq_id, slice, enq_flags);
	if (dsq_id == SHARED_DSQ) {
		__sync_fetch_and_add(&nr_eq_tasks, 1);
	}

	info_msg("[%s] -- %d -- %llu ns kicking cpu %d", __func__, p->pid,
		 slice, cpu);

	// let's wakeup the target cpu if it's idle - otherwise it would be noop
	scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE);
}

/*
 * Dispatch the user-space scheduler.
 */
static void dispatch_user_scheduler(void)
{
	struct task_struct *p;

	if (!test_and_clear_usersched_needed())
		return;

	p = bpf_task_from_pid(usersched_pid);
	if (!p) {
		scx_bpf_error("Failed to find usersched task %d",
			      usersched_pid);
		return;
	}
	/*
	 * Dispatch the scheduler on the first CPU available, likely the
	 * current one.
	 */
	dispatch_task(p, USCHED_CORE, 0, 0, USCHED_DSQ);
	bpf_task_release(p);
}

bool is_usersched_cpu(s32 cpu)
{
	return cpu == USCHED_CORE;
}

/*
   Select the target CPU where a task can be executed.
  
   Use scx_bpf_pick_any_cpu to pick from the only tsksz 
   cores that we want to schedule tasks on. 
*/
s32 BPF_STRUCT_OPS(tsksz_select_cpu, struct task_struct *p, s32 prev_cpu, u64 wake_flags)
{
	s32 cpu;
	callback_msg("%s", __func__);

    update_qid_as_per_e2e( p );

    print_task_cgroup_stats( p );

	s32 qid = task_to_qid(p);
	if (verify_qid(qid)) {
        struct bpf_cpumask __kptr *cpumask = qid_to_cpumask(qid);
        if ( cpumask ) {
            cpu = scx_bpf_pick_idle_cpu(
                                        (const struct cpumask *)cpumask,
                                        SCX_PICK_IDLE_CORE);
            if (0 <= cpu && cpu < MAX_CPUS) {
                info_msg("[%s] selected cpu %d for %d - %s",
                         __func__, cpu, p->pid, p->comm);
                scx_bpf_dispatch(p, SCX_DSQ_LOCAL,
                                 effective_slice_ns, 0);
                prev_cpu = cpu;
            }
            bpf_cpumask_release( cpumask );
        }
	} else {
		warn_msg("[%s] qid not found for %d - %s", __func__, p->pid,
			 p->comm);
	}

	warn_msg("[%s] get_best_cpu returned false", __func__);
	return prev_cpu;
}

/*
 * Task @p becomes ready to run. We can dispatch the task directly here if the
 * user-space scheduler is not required, or enqueue it to be processed by the
 * scheduler.
 */
void BPF_STRUCT_OPS(tsksz_enqueue, struct task_struct *p, u64 enq_flags)
{
	callback_msg("%s", __func__);

	/*
	 * Scheduler is dispatched directly in .dispatch() when needed, so
	 * we can skip it here.
     */
	if (is_usersched_task(p))
		return;

    print_task_cgroup_stats( p );



	s32 qid = task_to_qid(p);
	if (verify_qid(qid)) {
		scx_bpf_dispatch(p, qid, effective_slice_ns, 0);
	}
}

/*
 * Dispatch tasks that are ready to run.
 *
 * This function is called when a CPU's local DSQ is empty and ready to accept
 * new dispatched tasks.
 *
 * We may dispatch tasks also on other CPUs from here, if the scheduler decided
 * so (usually if other CPUs are idle we may want to send more tasks to their
 * local DSQ to optimize the scheduling pipeline).
 */
void BPF_STRUCT_OPS(tsksz_dispatch, s32 cpu, struct task_struct *prev)
{
	info_msg_fm_dispatch("[%s] on cpu -%d-", __func__,
		 cpu); // char *

	// Enqueue the scheduler task if the timer callback
	// has set the need flag - it's set every second
	dispatch_user_scheduler();

	if (is_usersched_cpu(cpu)) {
		scx_bpf_consume(USCHED_DSQ);
	}

	s32 qid = cpu_to_qid(cpu);
	if (verify_qid(qid)) {
		if (scx_bpf_consume(qid)) {
			info_msg_fm_dispatch("%s consumed a task from Q %d", __func__, qid);
		}
	}
}

/*
 * A new task @p is being created.
 *
 * Allocate and initialize all the internal structures for the task (this
 * function is allowed to block, so it can be used to preallocate memory).
 */
s32 BPF_STRUCT_OPS(tsksz_init_task, struct task_struct *p,
		   struct scx_init_task_args *args)
{
	callback_msg("%s", __func__);
	__sync_fetch_and_add(&nr_tasks, 1);

    print_task_cgroup_stats( p );

	struct task_group *tg = p->sched_task_group;
	if (tg && tg->css.cgroup) {
		struct cgroup *cgrp = tg->css.cgroup;
		info_msg("[%s] -- pid %d belongs to cgroup %d - %s", __func__,
			 p->pid, cgrp->kn->id, cgrp->kn->name);

		if (chashmap_present(cgrp->kn->id)) {
			queued_pids_push(p->pid);
			info_msg(
				"[%s][%s] -OK- pid %d belongs to cgroup %d - %s",
				__func__, p->comm, p->pid, cgrp->kn->id,
				cgrp->kn->name);
		} else {
			FETCH_KERNEL_STR(p->comm, name)
			const char *other = "gunicorn";
			int no = cus_strlen(other);
			n = n < no ? n : no;

			info_msg(
				"[%s][%s][%s][%d] -NO- pid %d belongs to cgroup %d - %s",
				__func__, p->comm, name, n, p->pid,
				cgrp->kn->id, cgrp->kn->name);

			if (match_prefix(other, name, n)) {
				// task name is gunicorn - so we are assuming it belongs to a
				// function

				info_msg(
					"[%s][%s] -RG- pid %d belongs to cgroup %d - %s",
					__func__, p->comm, p->pid, cgrp->kn->id,
					cgrp->kn->name);

				// put on ring buffer
				queued_pids_push(p->pid);

				// and insert the cgroup id for future reference
				FETCH_KERNEL_STR(cgrp->kn->name, name)
                chashmap_insert( cgrp->kn->id, name);
            }
		}
	}

    u64 stackptr = 0; 
    bpf_for_each_map_elem(&func_characs, func_characs_cb_print, &stackptr, 0); 

	return 0;
}

/*
 * Task @p is exiting.
 *
 * Notify the user-space scheduler that we can free up all the allocated
 * resources associated to this task.
 */
void BPF_STRUCT_OPS(tsksz_exit_task, struct task_struct *p,
		    struct scx_exit_task_args *args)
{
	callback_msg("%s", __func__);
	__sync_fetch_and_sub(&nr_tasks, 1);
}

/*
 * Heartbeat scheduler timer callback.
 *
 * If the system is completely idle the sched-ext watchdog may incorrectly
 * detect that as a stall and automatically disable the scheduler. So, use this
 * timer to periodically wake-up the scheduler and avoid long inactivity.
 *
 * This can also help to prevent real "stalling" conditions in the scheduler.
 */
static int usersched_timer_fn(void *map, int *key, struct bpf_timer *timer)
{
	int err = 0;

	/* Kick the scheduler */
	set_usersched_needed();

    // check all the dsqs - if anyone has any pending tasks 
    // kick the target cpus, so that we may not have any unnecessary stalls
    int i;
    s32 cpu;
    s32 n; 
    bpf_for(i, 0, SHARED_DSQ){
      n = scx_bpf_dsq_nr_queued( i );
      if ( n > 0 ){
        cpu = i*2;
        scx_bpf_kick_cpu( cpu, SCX_KICK_IDLE);
        scx_bpf_kick_cpu( cpu+1, SCX_KICK_IDLE);
      }
    }

	/* Re-arm the timer */
	err = bpf_timer_start(timer, NSEC_PER_SEC, 0);
	if (err)
		scx_bpf_error("Failed to arm stats timer");

	return 0;
}

/*
 * Initialize the heartbeat scheduler timer.
 */
static int usersched_timer_init(void)
{
	struct bpf_timer *timer;
	u32 key = 0;
	int err;

	timer = bpf_map_lookup_elem(&usersched_timer, &key);
	if (!timer) {
		scx_bpf_error("Failed to lookup scheduler timer");
		return -ESRCH;
	}
	bpf_timer_init(timer, &usersched_timer, CLOCK_BOOTTIME);
	bpf_timer_set_callback(timer, usersched_timer_fn);
	err = bpf_timer_start(timer, NSEC_PER_SEC, 0);
	if (err)
		scx_bpf_error("Failed to arm scheduler timer");

	return err;
}

/*
 * Create a single global DSQ.
 *
 * All the tasks are placed into it in a fifo manner.
 *
 * All cpus will pick a task one by one from it. 
 */
static int dsq_init(void)
{
	int err;
	int i;

	bpf_for(i, 0, SHARED_DSQ)
	{
		/* Create the global shared DSQ */
		err = scx_bpf_create_dsq(i, -1);
		if (err) {
			scx_bpf_error("failed to create shared DSQ: %d", err);
			return err;
		}
	}

	err = scx_bpf_create_dsq(USCHED_DSQ, -1);
	if (err) {
		scx_bpf_error("failed to create shared DSQ: %d", err);
		return err;
	}

	return 0;
}

/*
 * Initialize the scheduling class.
 */
s32 BPF_STRUCT_OPS_SLEEPABLE(tsksz_init)
{
	int err;

	info_msg("initializing the tsksz scheduler");

	/* Compile-time checks */
	BUILD_BUG_ON((MAX_CPUS % 2));

	/* Initialize tsksz core */
	err = dsq_init();
	if (err)
		return err;
	err = usersched_timer_init();
	if (err)
		return err;

	int i;
    s32 cpu;
	bpf_for(i, 0, SHARED_DSQ)
	{
		cpu = i * 2;
        cpu_to_qid_array[cpu] = i;
        cpu += 1;
        cpu_to_qid_array[cpu] = i;
	}
    
    // init thresholds for buckets 
    e2e_thresholds[0] = 2000;
    e2e_thresholds[1] = 4000;
    verify_get_groupid();
    
    // init the next qids array for each bucket 
    s32 gap = SHARED_DSQ / MAX_E2E_BUCKETS; // 6 
    s32 lower;

	bpf_for(i, 0, MAX_E2E_BUCKETS){
      lower = gap * i; // 0,6
      bkt_next_qid[i] = lower;
    }
    verify_gen_qid_new();

    // test qid groupid 
    verify_qid_to_groupid();

	return 0;
}

/*
 * Unregister the scheduling class.
 */
void BPF_STRUCT_OPS(tsksz_exit, struct scx_exit_info *ei)
{
	info_msg("exiting the tsksz scheduler");

	UEI_RECORD(uei, ei);
}

/*
 * Scheduling class declaration.
 */
SCX_OPS_DEFINE( tsksz_ops, 
           .select_cpu = (void *)tsksz_select_cpu,
	       .enqueue = (void *)tsksz_enqueue,
	       .dispatch = (void *)tsksz_dispatch,
	       .init_task = (void *)tsksz_init_task,
	       .exit_task = (void *)tsksz_exit_task,
	       .init = (void *)tsksz_init, 
           .exit = (void *)tsksz_exit,
	       .flags = SCX_OPS_ENQ_LAST | SCX_OPS_KEEP_BUILTIN_IDLE | SCX_OPS_SWITCH_PARTIAL,
	       .timeout_ms = 5000, 
           .name = "tsksz"
);


