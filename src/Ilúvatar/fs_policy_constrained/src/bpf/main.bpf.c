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

// global dsq id 
#define SHARED_DSQ MAX_CPUS
#define USCHED_DSQ SHARED_DSQ+1
#define USCHED_CORE 12  

/*
 * Scheduler attributes and statistics.
 */
u32 usersched_pid; /* User-space scheduler PID */
const volatile u64 slice_ns = SCX_SLICE_DFL; /* Base time slice duration */

/*
 * Effective time slice: allow the scheduler to override the default time slice
 * (slice_ns) if this one is set.
 */
volatile u64 effective_slice_ns;

// Number of tasks being handled by the bpf scheduler  
volatile u64 nr_tasks = 0;
volatile u64 nr_eq_tasks = 0;

// info msg with a specific tag 
#define info_msg(_fmt, ...) do {						\
    bpf_printk( "[info-constrained] " _fmt ,	\
    ##__VA_ARGS__);		\
} while(0)

#define callback_msg(_fmt, ...) do {						\
	s32 cpu = scx_bpf_task_cpu(p); \
    info_msg( _fmt " cpu: %d -- %d - %s", \
             ##__VA_ARGS__, \
             cpu, \
             p->pid, \
             p->comm ); \
} while(0)

// Maximum length of name (struct kernfs_node -> name)
#define MAX_NAME_LEN 10

// maximum number of tasks that can be handled 
#define MAX_ENQUEUED_TASKS 8192

// map of allocated CPUs.
const volatile s32 constrained_cores[MAX_CPUS];

// it is filled in during init from the constrained core array filled by
// userland 
#define CMASK_GLOBAL_KEY 0x0

struct cpumask_map_value {
        struct bpf_cpumask __kptr * cpumask;
};

struct array_map {
        __uint(type, BPF_MAP_TYPE_ARRAY);
        __type(key, int);
        __type(value, struct cpumask_map_value);
        __uint(max_entries, 1);
} constrained_cpumask_map SEC(".maps");

typedef struct cgroupvalue {
    char name[MAX_NAME_LEN];
} Cgroupvalue;

struct {
        __uint(type, BPF_MAP_TYPE_LRU_HASH);
        __uint(max_entries, 32);
        __type(key, __u64);
        __type(value, struct cgroupvalue);
} CgroupsHashMap SEC(".maps");

static void chashmap_insert (__u64 cid, char *name, int n)
{
  Cgroupvalue *cvalue = bpf_map_lookup_elem( &CgroupsHashMap, &cid );

  if (cvalue) {
      memcpy( cvalue->name, name, n);
  } else {
      Cgroupvalue new_cvalue; 
      memcpy( new_cvalue.name, name, n);
      bpf_map_update_elem(&CgroupsHashMap, &cid, &new_cvalue, BPF_NOEXIST);
  }
}

static bool chashmap_present (__u64 cid)
{
  Cgroupvalue *cvalue = bpf_map_lookup_elem( &CgroupsHashMap, &cid );
  if (cvalue) {
      return true;
  } 
  return false;
}

// For static list of func names 
#define MAXFUNCS 3 
struct func_data {
    char name[MAX_NAME_LEN];
};
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, MAXFUNCS);
	__type(key, u32);
	__type(value, struct func_data);
} funcs_list SEC(".maps");

static void funcs_list_insert (u32 idx, char *name, int n)
{
  struct func_data *fvalue = bpf_map_lookup_elem( &funcs_list, &idx );

  if (fvalue) {
      memcpy( fvalue->name, name, n );
  } else {
      struct func_data new_fvalue; 
      memcpy( new_fvalue.name, name, n );
      bpf_map_update_elem(&funcs_list, &idx, &new_fvalue, BPF_NOEXIST);
  }
}

static inline char * funcs_list_get (__u64 cid)
{
  struct func_data *fvalue = bpf_map_lookup_elem( &funcs_list, &cid );
  if (fvalue) {
      return fvalue->name;
  } 
  return NULL;
}

static inline bool match_prefix(const char *prefix, const char *str, u32 max_len)
{
	int c;
    if( !prefix  || !str ){
        return false;
    }

	bpf_for(c, 0, max_len) {
		if (prefix[c] == '\0')
			return true;
		if (str[c] != prefix[c])
			return false;
	}
	return false;
}

static inline bool match_prefix_kernel_str(const char *prefix, const char *kstr)
{
    char name[MAX_NAME_LEN];
    int n  = bpf_probe_read_kernel_str(name, MAX_NAME_LEN, kstr );
    n = n > MAX_NAME_LEN ? MAX_NAME_LEN : n;

    if ( n >= 0 && 
         match_prefix(  prefix, name, n )
        ){
        return true;
    }
    return false;
}

static __always_inline  int cus_strlen(const char *cs){
    int len = 0;
    while( cs != NULL && *cs != '\0' ){
        cs++;
        len++;
    }
    return len;
}

static int constrained_cpumask_map_insert(struct bpf_cpumask *mask, u32 key)
{
        struct cpumask_map_value local, *v;
        long status;
        struct bpf_cpumask *old;

        local.cpumask = NULL;
        status = bpf_map_update_elem(&constrained_cpumask_map, &key, &local, 0);
        if (status) {
                bpf_cpumask_release(mask);
                return status;
        }

        v = bpf_map_lookup_elem(&constrained_cpumask_map, &key);
        if (!v) {
                bpf_cpumask_release(mask);
                return -ENOENT;
        }

        old = bpf_kptr_xchg(&v->cpumask, mask);
        if (old)
                bpf_cpumask_release(old);

        return 0;
}

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

/*
 * Map of allocated CPUs.
 */
volatile u32 cpu_map[MAX_CPUS];

/*
 * Assign a task to a CPU (used in .running() and .stopping()).
 *
 * If pid == 0 the CPU will be considered idle.
 */
static void set_cpu_owner(u32 cpu, u32 pid)
{
	if (cpu >= MAX_CPUS) {
		scx_bpf_error("Invalid cpu: %d", cpu);
		return;
	}
    cpu_map[cpu] = pid;
}

/*
 * Get the pid of the task that is currently running on @cpu.
 *
 * Return 0 if the CPU is idle.
 */
static u32 get_cpu_owner(u32 cpu)
{
	if (cpu >= MAX_CPUS) {
		scx_bpf_error("Invalid cpu: %d", cpu);
		return 0;
	}
	return cpu_map[cpu];
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
 * Flag used to wake-up the user-space scheduler.
 */
static volatile u32 usersched_needed;

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
static void
dispatch_task(struct task_struct *p, s32 cpu, u64 task_slice_ns, u64 enq_flags, u64 dsq_id)
{
	u64 slice = task_slice_ns ? :
		__sync_fetch_and_add(&effective_slice_ns, 0) ? : slice_ns;
  
    // we only dispatch to a single global dsq
    scx_bpf_dispatch(p, dsq_id, slice, enq_flags);
    if( dsq_id == SHARED_DSQ ){
      __sync_fetch_and_add(&nr_eq_tasks, 1);
    }

    // let's wakeup the target cpu if it's idle - otherwise it would be noop 
    scx_bpf_kick_cpu(cpu, __COMPAT_SCX_KICK_IDLE);
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
		scx_bpf_error("Failed to find usersched task %d", usersched_pid);
		return;
	}
	/*
	 * Dispatch the scheduler on the first CPU available, likely the
	 * current one.
	 */
    dispatch_task( p, USCHED_CORE, 0, 0, USCHED_DSQ);
	bpf_task_release(p);
}

/*
   Find the best cpu to schedule the task on. 
   Currently it just tries to fetch first idle cpu. 
*/
bool get_best_cpu( s32 *rcpu) {
	s32 cpu;
    s32 key = CMASK_GLOBAL_KEY;
    const struct cpumask_map_value *v;

    v = bpf_map_lookup_elem(&constrained_cpumask_map, &key);
    if (v) {
      if ( v->cpumask ){
        cpu = scx_bpf_pick_any_cpu( (const struct cpumask *)v->cpumask, 
                                    SCX_PICK_IDLE_CORE
                                    );
        if ( 0 <= cpu && cpu < MAX_CPUS ){
          *rcpu = cpu;
          return true;
        }
      }
    }

    return false;
}

bool is_usersched_cpu( s32 cpu ){
    return cpu == USCHED_CORE;
}

bool is_constrained( s32 cpu ){
    s32 key = CMASK_GLOBAL_KEY;
    const struct cpumask_map_value *v;

    v = bpf_map_lookup_elem(&constrained_cpumask_map, &key);
    if (v) {
      if ( v->cpumask ){
           return bpf_cpumask_test_cpu(cpu, (const struct cpumask *)v->cpumask );
      }
    }
    return false;
}

/*
   Select the target CPU where a task can be executed.
  
   Use scx_bpf_pick_any_cpu to pick from the only constrained 
   cores that we want to schedule tasks on. 
*/
s32 BPF_STRUCT_OPS(constrained_select_cpu, struct task_struct *p, s32 prev_cpu,
		   u64 wake_flags)
{
	s32 cpu;

    callback_msg( "%s", __func__ );
    
    // dispatch the task to first idle cpu among constrained cpus 
    if ( get_best_cpu(&cpu) ){
        info_msg( "%s selected cpu %d", __func__, cpu );
        return cpu;
    }
    
    // otherwise put it in the global dsq 
    scx_bpf_dispatch(p, SHARED_DSQ, effective_slice_ns, 0);
	__sync_fetch_and_add(&nr_eq_tasks, 1);

	return prev_cpu;
}

/*
 * Task @p becomes ready to run. We can dispatch the task directly here if the
 * user-space scheduler is not required, or enqueue it to be processed by the
 * scheduler.
 */
void BPF_STRUCT_OPS(constrained_enqueue, struct task_struct *p, u64 enq_flags)
{
    s32 cpu; 
    
    // this callback shouldn't be called since we dispatch to the shared 
    // dsq in the select_cpu callback 
    callback_msg( "%s", __func__ );

	/*
	 * Scheduler is dispatched directly in .dispatch() when needed, so
	 * we can skip it here.
     */
    if (is_usersched_task(p))
        return;

    // dispatch the task to first idle cpu among constrained cpus 
    if ( get_best_cpu(&cpu) ){
        dispatch_task( p, cpu, 0, 0, SHARED_DSQ);
        return;
    }

    // otherwise we just put the task in shared dsq 
    scx_bpf_dispatch(p, SHARED_DSQ, effective_slice_ns, enq_flags);
	__sync_fetch_and_add(&nr_eq_tasks, 1);
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
void BPF_STRUCT_OPS(constrained_dispatch, s32 cpu, struct task_struct *prev)
{
    // Enqueue the scheduler task if the timer callback 
    // has set the need flag - it's set every second 
    dispatch_user_scheduler();
    
    if ( is_usersched_cpu( cpu ) ){
       scx_bpf_consume(USCHED_DSQ);
    }

    if ( is_constrained(cpu) ){
      // consume a task from the shared dsq	
      if ( scx_bpf_consume(SHARED_DSQ) ) {
        info_msg( "%s consumed on cpu: %d", __func__ , cpu );
        __sync_fetch_and_sub(&nr_eq_tasks, 1);
      } 
    }
}

/*
 * Task @p starts on its selected CPU (update CPU ownership map).
 */
void BPF_STRUCT_OPS(constrained_running, struct task_struct *p)
{
    callback_msg( "%s", __func__ );

	/*
	 * Mark the CPU as busy by setting the pid as owner (ignoring the
	 * user-space scheduler).
	 */
	//if (!is_usersched_task(p))
    //		set_cpu_owner(cpu, p->pid);
    
}

/*
 * Task @p stops running on its associated CPU (update CPU ownership map).
 */
void BPF_STRUCT_OPS(constrained_stopping, struct task_struct *p, bool runnable)
{
    callback_msg( "%s", __func__ );

	/*
	 * Mark the CPU as idle by setting the owner to 0.
	 */
	// if (!is_usersched_task(p)) {
	// 	set_cpu_owner(scx_bpf_task_cpu(p), 0);
	// }
}

/*
 * Task @p changes cpumask: update its local cpumask generation counter.
 */
void BPF_STRUCT_OPS(constrained_set_cpumask, struct task_struct *p,
                    const struct cpumask *cpumask)
{
  s32 cpu;
  callback_msg( "%s", __func__ );

  bpf_for( cpu, 0, MAX_CPUS ){
      if (bpf_cpumask_test_cpu(cpu, cpumask)){
        bpf_printk("task %s should be able to use CPU %d", p->comm, cpu);
      }
  }
}

/*
 * A CPU is taken away from the scheduler, preempting the current task by
 * another one running in a higher priority sched_class.
 */
void BPF_STRUCT_OPS(constrained_cpu_release, s32 cpu,
				struct scx_cpu_release_args *args)
{
	struct task_struct *p = args->task;

	/*
	 * If the interrupted task is the user-space scheduler make sure to
	 * re-schedule it immediately.
	 */
	if (is_usersched_task(p))
		set_usersched_needed();
}

/*
 * A new task @p is being created.
 *
 * Allocate and initialize all the internal structures for the task (this
 * function is allowed to block, so it can be used to preallocate memory).
 */
s32 BPF_STRUCT_OPS(constrained_init_task, struct task_struct *p,
		   struct scx_init_task_args *args)
{
    callback_msg( "%s", __func__ );
	__sync_fetch_and_add(&nr_tasks, 1);

    return 0;
}

/*
 * Task @p is exiting.
 *
 * Notify the user-space scheduler that we can free up all the allocated
 * resources associated to this task.
 */
void BPF_STRUCT_OPS(constrained_exit_task, struct task_struct *p,
		    struct scx_exit_task_args *args)
{
    callback_msg( "%s", __func__ );
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

	/* Create the global shared DSQ */
	err = scx_bpf_create_dsq(SHARED_DSQ, -1);
	if (err) {
		scx_bpf_error("failed to create shared DSQ: %d", err);
		return err;
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
s32 BPF_STRUCT_OPS_SLEEPABLE(constrained_init)
{
	int err;
    struct bpf_cpumask *mask;
    long status;
    s32 cpu; 

    info_msg("initializing the constrained scheduler");

	/* Compile-time checks */
	BUILD_BUG_ON((MAX_CPUS % 2));

	/* Initialize constrained core */
	err = dsq_init();
	if (err)
		return err;
	err = usersched_timer_init();
	if (err)
		return err;
  
    // initialize the element in the array map to null 
    mask = bpf_cpumask_create();
    if (!mask)
      return -ENOMEM;

    // set the cpumask based on constrained cores 
    bpf_for( cpu, 0, MAX_CPUS ){
        if ( constrained_cores[cpu] != 0 ) {
            bpf_cpumask_set_cpu( cpu, mask);
            bpf_printk("setting CPU %d as usable", cpu);
        }
    }
    
    if ( (status = constrained_cpumask_map_insert( mask, CMASK_GLOBAL_KEY )) ){
        return status;
    }

#define call_funcs_list_insert( i, s ) do { \
      funcs_list_insert( i, s, strlen(s) ); \
}while(false)
    s32 i = 0;
    call_funcs_list_insert( i++, "pyaes" );
    call_funcs_list_insert( i++, "rodina" );
    call_funcs_list_insert( i++, "gzip" );
    assert( i == MAXFUNCS );

	return 0;
}

/*
 * Unregister the scheduling class.
 */
void BPF_STRUCT_OPS(constrained_exit, struct scx_exit_info *ei)
{
    info_msg("exiting the constrained scheduler");

	UEI_RECORD(uei, ei);
}


s32 BPF_STRUCT_OPS(constrained_cgroup_init, struct cgroup *cgrp, struct scx_cgroup_init_args *args)
{
  info_msg(
           "[%s] -- %d -- %s", 
           __func__, 
           cgrp->kn->id,  // u64
           cgrp->kn->name ); // char * 
  
  s32 i;
  bpf_for( i, 0, MAXFUNCS ){
      if( match_prefix_kernel_str( funcs_list_get(i), cgrp->kn->name ) ){
          info_msg(
                   "[%s] -- %d -OK- %s", 
                   __func__, 
                   cgrp->kn->id, 
                   cgrp->kn->name );
      }
  }

  return 0;
}

void BPF_STRUCT_OPS(constrained_cgroup_exit, struct cgroup *cgrp)
{
    info_msg(
            "[%s] -- %d -- %s", 
             __func__, 
            cgrp->kn->id, 
            cgrp->kn->name );
}

/*
 * Scheduling class declaration.
 */
SCX_OPS_DEFINE( constrained,
	       .select_cpu		= (void *)constrained_select_cpu,
	       .enqueue			= (void *)constrained_enqueue,
	       .dispatch		= (void *)constrained_dispatch,
	       .running			= (void *)constrained_running,
	       .stopping		= (void *)constrained_stopping,
	       .set_cpumask		= (void *)constrained_set_cpumask,
	       .cpu_release		= (void *)constrained_cpu_release,
	       .init_task		= (void *)constrained_init_task,
	       .exit_task		= (void *)constrained_exit_task,
	       .cgroup_init		= (void *)constrained_cgroup_init,
	       .cgroup_exit		= (void *)constrained_cgroup_exit,
	       .init			= (void *)constrained_init,
	       .exit			= (void *)constrained_exit,
	       .flags			= SCX_OPS_ENQ_LAST | SCX_OPS_KEEP_BUILTIN_IDLE | SCX_OPS_SWITCH_PARTIAL,
	       .timeout_ms		= 5000,
	       .name			= "constrained" );

