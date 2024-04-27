/* Copyright (c) Abdul Rehman <abrehman@iu.ed> */
/* 
   Task Size Interval Assignment BPF Scheduler 
   that assigns specific Q to function cgroups  
   based on their metadata. 
    
   Each Q dispatches tasks to fixed pair of cores.  
   Wichever is idle, giving power of 2 choice. 

   Tasks run for a fixed timeslice.

   This software may be used and distributed according to the terms of the
   GNU General Public License version 2.
 */
#include <scx/common.bpf.h>
#include <scx/ravg_impl.bpf.h>

#include <errno.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include "intf.h"

char _license[] SEC("license") = "GPL";

UEI_DEFINE(uei);

////////////////////////////////////////
// Structures 

typedef struct CharVal{
    u32 prio;
    u32 e2e;
    u32 loc;
} MetaVal_t;

// let's create a hashmap 
// a hash map 
struct {
  __uint(type, BPF_MAP_TYPE_HASH);
  __uint(max_entries, MAX_FUNCS);
  __uint(key_size, sizeof(char)*FUNC_METADATA_KEYSIZE);         /* cgrp ID */
  __uint(value_size, sizeof(MetaVal_t)); /* Value Structure */
} func_metadata SEC(".maps");

typedef struct CgroupInfo {
    u64 id;
	s32 qid;
    s32 tsk_cnt;
	char name[MAX_NAME_LEN];
} CgroupInfo_t;

/*
   HashMap to keep track of the Cgroups whose 
   tasks we are capturing.
*/
struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, MAX_CGROUPS);
	__type(key, __u64);
	__type(value, CgroupInfo_t);
} CgroupsHashMap SEC(".maps");

typedef struct TaskInfo {
    s32 pid;
	s32 qid_cur;
    CgroupInfo_t *cgroupctx;
} TaskInfo_t;

/*
   HashMap to keep track of the task context.
*/
struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, MAX_ENQUEUED_TASKS);
	__type(key, __s32);                      // pid of the task 
	__type(value, TaskInfo_t);               // context of the task 
} TasksHashMap SEC(".maps");

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

stats_t global_stats;

struct lock_wrapper {
	struct bpf_spin_lock lock;
};

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__type(key, u32);
	__type(value, struct lock_wrapper);
	__uint(max_entries, 1);
	__uint(map_flags, 0);
} global_locks SEC(".maps");

// The map containing stats that are to be switched to SchedEXT policy.
// it is drained by the user space thread
struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, MAX_ENQUEUED_TASKS);
} queued_stats SEC(".maps");

/////////////////////////////////////////////
// Global Variables 

/*
 * Scheduler attributes and statistics.
 */
u32 usersched_pid; /* User-space scheduler PID */

/*
 * Effective time slice: allow the scheduler to override the default time slice
 * (slice_ns) if this one is set.
 */
// const volatile u64 effective_slice_ns = SCX_SLICE_DFL; /* SCX_SLICE_DFL is 20 ms */
const volatile u64 effective_slice_ns = 10 * ONE_MSEC;

// Number of tasks being handled by the bpf scheduler
volatile u64 nr_tasks = 0;
volatile u64 nr_eq_tasks = 0;

/*
                                                               
Buckets / Groups                                                               

  Unassigned        0            2000           4000           INF  -- Thresholds

  │ Group Reserved  │  Group 1     │ Group 2      │  Group Rest │
  │                 │              │              │             │
  │   No E2E        │              │              │  all > 4000 │
  │                 │              │              │             │
  │  ┌┐ ┌┐ ┌┐ ┌┐    │ ┌┐ ┌┐ ┌┐ ┌┐  │ ┌┐ ┌┐ ┌┐ ┌┐  │ ┌┐ ┌┐ ┌┐ ┌┐ │
  │  ││ ││ ││ ││    │ ││ ││ ││ ││  │ ││ ││ ││ ││  │ ││ ││ ││ ││ │
  │  ││ ││ ││ ││    │ ││ ││ ││ ││  │ ││ ││ ││ ││  │ ││ ││ ││ ││ │
  │  ││ ││ ││ ││    │ ││ ││ ││ ││  │ ││ ││ ││ ││  │ ││ ││ ││ ││ │
  │  └┘ └┘ └┘ └┘    │ └┘ └┘ └┘ └┘  │ └┘ └┘ └┘ └┘  │ └┘ └┘ └┘ └┘ │
                                                               
*/

// Thresholds for each bucket. 
volatile u32 e2e_thresholds[MAX_E2E_BUCKETS -2]; // we don't need thresholds
                                                 // for reserved and rest
                                                 // buckets
// Next Qid to assign for a given bucket 
// it's initialized in init_scheduler
static volatile s32 bkt_next_qid[MAX_E2E_BUCKETS] = {0};

/*
 * Flag used to wake-up the user-space scheduler.
 */
static volatile u32 usersched_needed;

// verbose debug output flag
bool verbose = true;

/////////////////////////////////
// Function Declarations 

static __always_inline CgroupInfo_t   get_task_cgroupinfo( struct task_struct *p );
static __always_inline CgroupInfo_t * get_chashmap( __u64 cid );
static __always_inline MetaVal_t *    get_func_metadata( char *key_name );
static __always_inline bool verify_qid( s32 qid );

void scx_bpf_switch_to_scx(struct task_struct *p) __ksym;
struct kernfs_node *bpf_get_parent_kernfs(struct cgroup *cgrp) __ksym;
void bpf_release_kernfs(struct kernfs_node *kn) __ksym;

/////////////////////////////////
// Function Definitions 

static __always_inline char readchar( const char *str, u32 index )
{
    char c;
    bpf_probe_read_kernel( &c, sizeof(c), str + index );
    return c;
}

static __always_inline bool str_is_docker( const char *str )
{
                  // 0123456
    char * docker = "docker";
    int i;
    bool is_match = true;

    if (!str) {
        return false;
    }

    bpf_for(i, 0, 6){
        char c = readchar( str, i );
        if ( c == '\0' ){
            is_match = false;
            break;
        }
        if (docker[i] != c){
            is_match = false;
            break;
        }
    }
    return is_match;
}

static __always_inline bool is_docker_parent( struct cgroup *cgrp )
{
    struct kernfs_node *parent;
    bool result;

    parent = bpf_get_parent_kernfs( cgrp );
    if (!parent){
      return false;
    }

    result = str_is_docker( parent->name );

    bpf_release_kernfs( parent );
    return result;
}

void global_stats_update_tsks_Q( s32 qid, s32 tsks_cnt ){
    if( verify_qid(qid) ){
        global_stats.tsks_Q[qid] = tsks_cnt;
    }
}

void global_stats_add_tsks_Q( s32 qid, s32 tsks_cnt ){
    if( verify_qid(qid) ){
        __sync_fetch_and_add( &global_stats.tsks_Q[qid], tsks_cnt );
    }
}

void global_stats_sub_tsks_Q( s32 qid, s32 tsks_cnt ){
    if( verify_qid(qid) ){
        __sync_fetch_and_sub( &global_stats.tsks_Q[qid], tsks_cnt );
    }
}

s32 get_groupid( u32 e2e ) {
    int i;

    // can setup if else branch for thresholds 
    if( e2e == 0 ){
        return RESERVED_E2E_BUCKET;
    };

    if( e2e < e2e_thresholds[0] ) {
        return 1;
    } else if ( e2e < e2e_thresholds[1] ) {
        return 2;
    }

    return 3;
}

/*
   Test Results 
      root@v-021:/data2/ar/workspace/temp# cat /sys/kernel/debug/tracing/trace_pipe | grep -i test
      fs_policy_tsksz-2481710 [002] ...11 227122.741565: bpf_trace_printk: [info-tsksz] [test][get_groupid] e2e: 0 -> gid 0 -- should be 0 -- passed: 1
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
    
    TESTCASE_get_groupid( 0, RESERVED_E2E_BUCKET )
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

static __always_inline void update_qid_assignment( struct task_struct *p ) {
    
    // get the cgroup to which this task belongs to 
    CgroupInfo_t cgrp = get_task_cgroupinfo( p );
    
    // fetch associated metadata of the function 
    MetaVal_t *fmeta = get_func_metadata( cgrp.name );
    if ( fmeta ){
        
        // verify if we should be updating the group assignment  
        // of this cgroup using chashmap 
        CgroupInfo_t * cgrp_old = get_chashmap( cgrp.id );  
        if ( cgrp_old ){
              

              // Update based on the e2e changes 
              s32 gid  = get_groupid( fmeta->e2e ); // new group id 
              s32 ogid = qid_to_groupid( cgrp_old->qid ); // old group id 

              if ( ogid != gid ){
                  s32 nqid; 

                  nqid = gen_qid_new( gid );

                  info_msg( "[qid_assignment] cgroup %d - %s now is assigned Q %d instead of old-Q %d", 
                           cgrp_old->id,         
                           cgrp_old->name,         
                           nqid,
                           cgrp_old->qid
                  );

                  cgrp_old->qid = nqid;

                  // todo: it's an abrupt change in stats - even though actual
                  // shift only happens when tasks go through the select_cpu
                  // callback  
                  // so there is a lag between stats and actual shift - but it
                  // should be very small for coarse grained observation over a
                  // second 
              } else {
                  // if there were not changes based on the e2e 
                  // we will try load balancing based on Q threshold within the
                  // same group 

                  s32 qlen = scx_bpf_dsq_nr_queued( cgrp_old->qid ); 
                  if ( qlen > QMAX_THRESHOLD ){

                      s32 nqid; 
                      nqid = gen_qid_new( gid );

                      info_msg( "[qid_assignment][load_balanced] cgroup %d - %s now is assigned Q %d instead of old-Q %d", 
                               cgrp_old->id,         
                               cgrp_old->name,         
                               nqid,
                               cgrp_old->qid
                      );

                      cgrp_old->qid = nqid;
                  }
              }
        } // cgrp_old 
    } // fmeta
}

/*
   Callback for bpf_for_each_map_elem
   long (\*callback_fn)(struct bpf_map \*map, const void \*key, void \*value, void \*ctx);
   callback  
      continues : if return 0
      stops     : if return 1
*/
static long func_metadata_dump_callback (void *map, const char *key, MetaVal_t *val, void *data){
    info_msg("[func_metadata][dump_callback] key: %s e2e: %lu", 
             key,
             val->e2e
    ); 
    return 0;
}

/*
   Callback for bpf_for_each_map_elem
   long (\*callback_fn)(struct bpf_map \*map, const void \*key, void \*value, void \*ctx);
   callback  
      continues : if return 0
      stops     : if return 1
*/
static long func_cgroup_dump_callback (void *map, const __u64 *key, CgroupInfo_t *val, void *data){
    info_msg("[func_cgroup][dump_callback] key: %d id: %llu qid: %d tasks: %d name: %s", 
             *key,
             val->id,
             val->qid,
             val->tsk_cnt,
             val->name
    ); 
    return 0;
}

static __always_inline MetaVal_t * get_func_metadata( char *key_name )
{
	MetaVal_t *cvalue = bpf_map_lookup_elem(&func_metadata, key_name);
	return cvalue;
}

static __always_inline bool verify_qid( s32 qid )
{
	if ( 0 <= qid && qid < SHARED_DSQ ) {
		return true;
	}
	return false;
}

static __always_inline bool verify_cpu(s32 cpu)
{
	if ( 0 <= cpu && cpu < MAX_CPUS ) {
		return true;
	}
	return false;
}

static __always_inline s32 cpu_to_qid(s32 cpu)
{
    return (u32)cpu / 2;
}

// Generate a bpf cpumask with cpus that belong to qid
// cpumask needs to be released after use 
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

void push_pid_for_class_switch( int pid )
{
	int *p = bpf_ringbuf_reserve( &queued_pids, sizeof(packet_pid_t), 0 );
	if ( p ) {
		packet_pid_t *ps = (packet_pid_t *)p;
		ps->pid = pid;
		bpf_ringbuf_submit(ps, 0);
        info_msg( "[queued_pids] pushed pid %d", pid );
	}
}

void push_stats( stats_t *stat )
{
    if( !stat ){
        return;
    }

	int *p = bpf_ringbuf_reserve( &queued_stats, sizeof(stats_t), 0 );
	if ( p ) {
		stats_t *ps = (stats_t *)p;
        stat->timestamp_ms = bpf_ktime_get_ns()/ONE_MSEC;
        __builtin_memcpy_inline( ps, stat, sizeof(stats_t) );
		bpf_ringbuf_submit(ps, 0);
        info_msg( "[queued_stats] pushed stat %p", ps );
	}
} 

static void chashmap_insert( CgroupInfo_t *cgrp )
{
	CgroupInfo_t *cgrp_old = bpf_map_lookup_elem( &CgroupsHashMap, &cgrp->id );

	if ( cgrp_old ) {
		__builtin_memcpy_inline(cgrp_old->name, cgrp->name, MAX_NAME_LEN);
        cgrp_old->id = cgrp->id;
        cgrp_old->qid = cgrp->qid;
	} else {
		CgroupInfo_t cgrp_new;

		__builtin_memcpy_inline( &cgrp_new, cgrp, sizeof(CgroupInfo_t) );

        // we are assumming right now that we don't have any info about 
        // the associated function metadata  
		cgrp_new.qid = gen_qid_new( RESERVED_E2E_BUCKET );

        bpf_map_update_elem( 
                            &CgroupsHashMap, 
                            &cgrp->id, 
                            &cgrp_new,
                            BPF_NOEXIST
        );

		info_msg("[chashmap] inserting cgroup %d - %s with Q %d", 
                 cgrp->id,
                 cgrp->name,
                 cgrp_new.qid
        );
	}
}

static __always_inline CgroupInfo_t *get_chashmap(__u64 cid)
{
	CgroupInfo_t *cvalue = bpf_map_lookup_elem(&CgroupsHashMap, &cid);
	return cvalue;
}

static __always_inline CgroupInfo_t get_task_cgroupinfo(struct task_struct *p)
{
    struct cgroup *cgrp;
    CgroupInfo_t info;

    bpf_rcu_read_lock();
      // cgroups->dfl_cgrp is the cgroup-id 1 
      // we need cgroups->subsys[sched] cgroup 
      if( p->sched_task_group && 
          p->sched_task_group->css.cgroup 
         ){
          cgrp = p->sched_task_group->css.cgroup;
          info.id = cgrp->kn->id;
          bpf_probe_read_kernel_str( info.name, MAX_NAME_LEN, cgrp->kn->name );
      } 
    bpf_rcu_read_unlock();

    info.qid = -1;

    return info;
}


static __always_inline void thashmap_insert( TaskInfo_t *tinfo )
{
	TaskInfo_t *tinfo_old = bpf_map_lookup_elem( &TasksHashMap, &tinfo->pid );
    long r = bpf_map_update_elem( 
                        &TasksHashMap, 
                        &tinfo->pid, 
                        tinfo,
                        BPF_ANY
    );
    info_msg("[thashmap] inserting task %d with Q %d - status %d", 
             tinfo->pid,
             tinfo->qid_cur,
             r
    );
}

static __always_inline TaskInfo_t* get_task_ctx(s32 pid)
{
	TaskInfo_t *tinfo = bpf_map_lookup_elem(&TasksHashMap, &pid);
	return tinfo;
}

static __always_inline s32 task_to_qid(struct task_struct *p)
{
    CgroupInfo_t cgrp = get_task_cgroupinfo( p );

    CgroupInfo_t * cvalue = get_chashmap( cgrp.id );
    if (cvalue) {
        return cvalue->qid;
    }else{
        // sometimes task switch to sched_ext even after partial being set
        // let's sched those tasks through qid 0 
        info_msg("[warn] no qid found for task %d - %s belongs to cgroup %d - %s",
                 p->pid,
                 p->comm,
                 cgrp.id,
                 cgrp.name
        );
        return 0;
    }

    return -1;
}

// todo: make this ugly function better 
static inline bool match_prefix(const char *prefix, const char *str, u32 max_len)
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

// todo: remove if not needed
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
static inline bool is_usersched_task( const struct task_struct *p )
{
	return p->pid == usersched_pid;
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
	scx_bpf_dispatch( p, USCHED_DSQ, effective_slice_ns, 0 );
	scx_bpf_kick_cpu( USCHED_CORE, SCX_KICK_IDLE );

	bpf_task_release(p);
}

static __always_inline inline bool is_usersched_cpu(s32 cpu)
{
	return cpu == USCHED_CORE;
}

/*
   Select the target CPU where a task can be executed.
  
   We use scx_bpf_pick_idle_cpu(...) to pick idle CPU from 
   among the CPUs that belong to the given Qid. 

   Currently we associate two CPUs to each Q resulting in a 
   power of 2 choice. 
*/
s32 BPF_STRUCT_OPS(tsksz_select_cpu, struct task_struct *p, s32 prev_cpu, u64 wake_flags)
{
	s32 cpu;
    
    // Update the Qid assignment based on the 
    // current snapshot of the function characteristics 
    // BPF map 
    update_qid_assignment( p );

	s32 qid = task_to_qid( p );
	if ( verify_qid(qid) ){

        TaskInfo_t *taskctx = get_task_ctx( p->pid );
        if ( taskctx ){
          taskctx->qid_cur = qid;
        }

        struct bpf_cpumask __kptr *cpumask = qid_to_cpumask(qid);
        if ( cpumask ) {
            cpu = scx_bpf_pick_idle_cpu(
                                        (const struct cpumask *)cpumask,
                                        SCX_PICK_IDLE_CORE);
            if ( 0 <= cpu && cpu < MAX_CPUS ) {
                info_msg( "[select_cpu] selected cpu %d for task: %d - %s",
                          cpu, 
                          p->pid, 
                          p->comm
                );
                // we immediately dispatch to the local DSQ of the idle CPU
                scx_bpf_dispatch( p, SCX_DSQ_LOCAL, effective_slice_ns, 0 );
                prev_cpu = cpu;
            }
            bpf_cpumask_release( cpumask );
        }
	} else {
      CgroupInfo_t cgrp = get_task_cgroupinfo( p );
      info_msg( "[select_cpu] Q id not found for task: %d - %s belongs to cgroup: %d - %s",
                p->pid, 
                p->comm,
                cgrp.id,
                cgrp.name
      );
	}
    
    // if there is no dispatch to Local DSQ enqueue callback would be called
    // where we enqueue to a custom DSQ
    // later when dispatch callback is called we consume from the 
    // custom DSQ into Local DSQ of that CPU on which that dispatch was called
	return prev_cpu;
}

/*
 * Task @p becomes ready to run. We can dispatch the task directly here if the
 * user-space scheduler is not required, or enqueue it to be processed by the
 * scheduler.
 */
void BPF_STRUCT_OPS(tsksz_enqueue, struct task_struct *p, u64 enq_flags)
{
    info_msg("[enqueue] task: %d - %s EFlags: 0x%llx",
             p->pid,
             p->comm,
             enq_flags
    );

	/*
	 * Scheduler is dispatched directly in .dispatch() when needed, so
	 * we can skip it here.
     */
	if ( is_usersched_task(p) )
		return;

	s32 qid = task_to_qid(p);
	if (verify_qid(qid)) {
        TaskInfo_t *taskctx = get_task_ctx( p->pid );
        if( taskctx ){
          taskctx->qid_cur = qid;
        }

		scx_bpf_dispatch( p, qid, effective_slice_ns, 0 );
        info_msg("[enqueue] enqueued task: %d - %s to Q %d",
                 p->pid,
                 p->comm,
                 qid 
        );
        global_stats_update_tsks_Q( qid, scx_bpf_dsq_nr_queued(qid) );

        // trigger a follow up scheduling event 
        s32 cpu = qid*2;
        scx_bpf_kick_cpu( cpu   , SCX_KICK_IDLE);
        scx_bpf_kick_cpu( cpu+1 , SCX_KICK_IDLE);
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
    if( prev ){
        info_msg("[dispatch] cpu: %d prev_task: %d - %s",
                 cpu,
                 prev->pid,
                 prev->comm
        );
    }

    // Enqueue the scheduler task if the timer callback
    // has set the need flag - it's set every second
	dispatch_user_scheduler();

	if ( is_usersched_cpu(cpu) ) {
        // dispatches from custom DSQ to Local DSQ of this cpu 
        if ( scx_bpf_consume(USCHED_DSQ) ){
            info_msg("[dispatch] consumed user sched Q on cpu: %d ",
                     cpu
            );
        }
	}

    // dispatch tasks from custom DSQ that corresponds to this CPU 
    // to Local DSQ of this cpu 
	s32 qid = cpu_to_qid( cpu );
	if ( verify_qid(qid) ) {
		if ( scx_bpf_consume(qid) ) {
            info_msg("[dispatch] consumed Q %d on cpu: %d ",
                     qid,
                     cpu
            );
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
    bool push_to_ringbuf = false;

	info_msg("[init_task] initializing task %d - %s",
             p->pid,
             p->comm
    );
	__sync_fetch_and_add(&nr_tasks, 1);

    if( p->sched_task_group  ){
      struct cgroup *cgrp;      
  
      cgrp = p->sched_task_group->css.cgroup;
  
      if ( cgrp && is_docker_parent( cgrp ) ){
          scx_bpf_switch_to_scx( p );
          bpf_printk("[cgroup-prog][cgroup][switch] switched %s - %d belongs to name %s", 
                     p->comm,
                     p->pid,
                     cgrp->kn->name
          );

          CgroupInfo_t *cgrp_old = get_chashmap( cgrp->kn->id );
          if ( cgrp_old ) {
              LOCK_HEADER( STATS_LOCK ) {
                  bpf_spin_lock(&lockw->lock);
                  __sync_fetch_and_add( &cgrp_old->tsk_cnt, 1 );
                  bpf_spin_unlock(&lockw->lock);
              }

              TaskInfo_t taskctx;
              taskctx.pid = p->pid;
              taskctx.qid_cur = cgrp_old->qid;
              taskctx.cgroupctx = cgrp_old;
              thashmap_insert( &taskctx );
          } else {
              CgroupInfo_t cgrpinfo;
              cgrpinfo.tsk_cnt = 0;
              cgrpinfo.id = cgrp->kn->id;
              chashmap_insert( &cgrpinfo );
          }
      }
    }
  
    if ( verbose ){
      // dump out the function characteritics map 
      u64 stackptr = 0; 
      bpf_for_each_map_elem(
            &func_metadata, 
            func_metadata_dump_callback, 
            &stackptr, 
            0
      ); 
    }

	return 0;
}

/*
 * Task @p is exiting.
 */
void BPF_STRUCT_OPS(tsksz_exit_task, struct task_struct *p,
		    struct scx_exit_task_args *args)
{
	info_msg("[exit_task] exiting task %d - %s",
             p->pid,
             p->comm
    );
	__sync_fetch_and_sub(&nr_tasks, 1);

    CgroupInfo_t cgrp = get_task_cgroupinfo( p );
    CgroupInfo_t *cgrp_old = get_chashmap(cgrp.id);
    if( cgrp_old ){
      LOCK_HEADER( STATS_LOCK ) {
        bpf_spin_lock(&lockw->lock);
          __sync_fetch_and_sub( &cgrp_old->tsk_cnt, 1 );
        bpf_spin_unlock(&lockw->lock);
      }
    }

    TaskInfo_t *tinfo = get_task_ctx( p->pid );
    if( tinfo ){
      info_msg("[thashmap] exiting task %d with Q %d ", 
               tinfo->pid,
               tinfo->qid_cur
      );
    }
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
      global_stats_update_tsks_Q( i, n );
      if ( n > 0 ){
        cpu = i*2;
        scx_bpf_kick_cpu( cpu, SCX_KICK_IDLE);
        scx_bpf_kick_cpu( cpu+1, SCX_KICK_IDLE);
      }
    }
 
    push_stats( &global_stats );

    if ( verbose ){

      info_msg("[health] heartbeat message");

      int i;
      bpf_for(i, 0, SHARED_DSQ) {
          info_msg("[stats] q[%d] -> %d",
                   i,
                   global_stats.tsks_Q[i]
          );
      }

      // dump out the cgrouphashmap  
      u64 stackptr = 0; 
      bpf_for_each_map_elem(
            &CgroupsHashMap, 
            func_cgroup_dump_callback, 
            &stackptr, 
            0
      ); 

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
   Init the DSQs

   Since we are using scx_bpf_dispatch(...) to dispatch to 
   the custom DSQs, they are being used as FIFOs instead of 
   priority Qs. 
 */
static int dsq_init(void)
{
	int err;
	int i;
    
    // create SHARED_DSQ number of custom DSQs 
	bpf_for(i, 0, SHARED_DSQ)
	{
		err = scx_bpf_create_dsq(i, -1);
		if (err) {
			scx_bpf_error("failed to create shared DSQ: %d", err);
			return err;
		}
	}
    
    // create a separate DSQ for the user space scheduler thread
	err = scx_bpf_create_dsq(USCHED_DSQ, -1);
	if (err) {
		scx_bpf_error("failed to create shared DSQ: %d", err);
		return err;
	}

	return 0;
}

/*
    Initialize the scheduling class.
*/
s32 BPF_STRUCT_OPS_SLEEPABLE(tsksz_init)
{
	int err;

	info_msg("[init] initializing the tsksz scheduler");

	/* Compile-time checks */
	BUILD_BUG_ON((MAX_CPUS % 2));
    
    // init dsqs 
	err = dsq_init();
	if (err)
		return err;
    
    // arm the timer callback 
	err = usersched_timer_init();
	if (err)
		return err;
    
    // init thresholds for buckets 
    e2e_thresholds[0] = 2000; // 2 seconds 
    e2e_thresholds[1] = 4000; // 4 seconds 
    
    // init the next qids array for each bucket 
    s32 gap = SHARED_DSQ / MAX_E2E_BUCKETS; // 6 
    s32 lower;
    s32 i;
    bpf_for(i, 0, MAX_E2E_BUCKETS){
        lower = gap * i; // 0,6
        bkt_next_qid[i] = lower;
    }

    if ( verbose ){
        // testcases for each function 
        verify_get_groupid();
        verify_gen_qid_new();
        verify_qid_to_groupid();
    }

	return 0;
}

/*
 * Unregister the scheduling class.
 */
void BPF_STRUCT_OPS(tsksz_exit, struct scx_exit_info *ei)
{
	info_msg("[exit] exiting the tsksz scheduler");


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
