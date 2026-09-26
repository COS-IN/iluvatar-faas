#ifdef LSP
#ifndef __bpf__
#define __bpf__
#endif
#define LSP_INC
#include "../../../include/scx/common.bpf.h"
#else
#include <scx/common.bpf.h>
#endif

#include "intf.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

// we don't want licence of hashmap to be included for the scheduler
#ifndef __LICENSE_H
#define __LICENSE_H
char _license[] SEC("license") = "GPL";
#endif

//////////////////////////////
// Global Data for bpf scheduler

bool cpu_boost_config = false;
bool enable_timer_callback = false;
bool use_switch_kfunc = true;
bool enable_task_ip_monitoring = true;
u32 nr_cpu_ids_config = MAX_CPUS;
u32 enqueue_config = SCHED_CONFIG_PRIO_DSQ;
u64 domains_count = 0;
u64 last_max_vtime = 0;

struct GlobalStats {
    u64 task_count;
};
struct GlobalStats global_stats;

SchedGroupChrs_t empty_sched_chrs = {0};

private(FINESCHED) struct bpf_cpumask __kptr *cores_inact_grp_mask;
u8 cores_inact_grp[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
// u8 cores_inact_grp[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
// 20, 21, 22, 23 };

private(FINESCHED) struct bpf_cpumask __kptr *cpumask_node0;
u8 cores_node0[] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                    12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};

private(FINESCHED) struct bpf_cpumask __kptr *cpumask_node1;
u8 cores_node1[] = {24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
                    36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47};

struct cpucycles_ctx {
    u64 last_timestamp;
    u64 cycles_counter;
    u64 cycles_per_sec;
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, u32);
    __type(value, struct cpucycles_ctx);
    __uint(max_entries, 1);
} HardwareCpuCyclesMap SEC(".maps");

/*
 * Return per CPU Perf context.
 */
__always_inline struct cpucycles_ctx *try_lookup_cpucycles_ctx(s32 cpu) {
    const u32 idx = 0;
    return bpf_map_lookup_percpu_elem(&HardwareCpuCyclesMap, &idx, cpu);
}

// cgroup characteristics shared map copy
// to avoid missed lookup when user is writing
// while bpf side is trying to read.
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, MAX_MAP_ENTRIES_CMAP);
    __uint(key_size, sizeof(char) * MAX_PATH); // key: cgroup name
    __uint(value_size, sizeof(CgroupChrs_t));  // value: cgroup characteristics
} cMapLast SEC(".maps");

/*
 * Per-CPU context.
 */
struct cpu_ctx {
    u64 prio_dsqid; // implicitly the sched domain this cpu belongs to
    u64 last_vtime;

    // idle time tracking
    u64 idle_start;
    u64 idle_time;

    // utilization collection
    u64 last_calc_time;
    u64 prev_idle_time;
    u64 util;
    u64 avg_util;
};

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, u32);
    __type(value, struct cpu_ctx);
    __uint(max_entries, 1);
} cpu_ctx_stor SEC(".maps");

/*
 * Return a CPU context.
 */
__always_inline struct cpu_ctx *try_lookup_cpu_ctx(s32 cpu) {
    const u32 idx = 0;
    return bpf_map_lookup_percpu_elem(&cpu_ctx_stor, &idx, cpu);
}

/*
 * Per-task local storage.
 *
 * This contain all the per-task information used internally by the BPF code.
 */
struct task_context {
    u64 vtime;

    u64 running_start_time;
    u64 cpu_time_avg;

    u64 last_wakeup_time;
    u64 wakeup_roundtrip_time_avg;

    u64 sleeps_count;
    u64 last_sleep_freq_update_time;
    u64 sleep_freq_avg;

    u64 enqueue_count;

    bool is_worker;
    bool use_specified_cpus;

    u64 ip_mean;
    u64 ip_variance;
    u64 ip_sample_count;
    u64 busypolling_factor;
};

// Per-CPU task context:
//    Each cpu maintains it's own history about tasks. The helper
//    returns current cpu's history of the tasks.

// Global task context:
//    Each cpu updates and get's tasks history from
//    single task_storage map without synchronization.
#define USE_GLOBAL_TASK_CONTEXT_STORAGE 1

#if USE_GLOBAL_TASK_CONTEXT_STORAGE
/* Map that contains task context globally for each cpu.*/
struct {
    __uint(type, BPF_MAP_TYPE_TASK_STORAGE);
    __uint(map_flags, BPF_F_NO_PREALLOC);
    __type(key, int);
    __type(value, struct task_context);
} task_ctx_stor SEC(".maps");

/*
 * Return a local task context from a generic task.
 */
struct task_context *try_lookup_task_ctx(const struct task_struct *p) {
    return bpf_task_storage_get(&task_ctx_stor, (struct task_struct *)p, 0,
                                BPF_LOCAL_STORAGE_GET_F_CREATE);
}
#else
/* Map that contains per-cpu task context. */
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, MAX_MAP_ENTRIES_TASK_STORE);
    __type(key, s32); // pid
    __type(value, struct task_context);
} task_ctx_stor SEC(".maps");

/*
 * Return a local task context from a generic task.
 */
struct task_context *try_lookup_task_ctx(const struct task_struct *p) {
    s32 err;
    s32 pid = p->pid;
    pid = pid % MAX_MAP_ENTRIES_TASK_STORE; // makes stats for some tasks bad

    struct task_context *context = bpf_map_lookup_elem(&task_ctx_stor, &pid);

    if (!context) {
        struct task_context default_context;
        memset(&default_context, 0, sizeof(default_context));

        err = bpf_map_update_elem(&task_ctx_stor, &pid, &default_context, BPF_NOEXIST);
        if (err < 0) {
            dbg("[task_stats][task_context_store] error(%d) failed to update elem in map", err);
            return NULL;
        }

        context = bpf_map_lookup_elem(&task_ctx_stor, &pid);
    }

    return context;
}
#endif

/*
 * Per-dsq map.
 *
 * This contain all the per-dsq context information.
 */
struct dsq_context {
    u64 last_consume_time;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, MAX_MAP_ENTRIES_CMAP);
    __uint(key_size, sizeof(u64));                  // key: dsqid
    __uint(value_size, sizeof(struct dsq_context)); // value: dsq_context
} dsq_context_store SEC(".maps");

static __noinline struct dsq_context *try_lookup_dsq_context(u64 dsqid) {
    s32 err;

    struct dsq_context *context = bpf_map_lookup_elem(&dsq_context_store, &dsqid);
    if (!context) {
        struct dsq_context default_context;
        memset(&default_context, 0, sizeof(default_context));

        err = bpf_map_update_elem(&dsq_context_store, (const void *)&dsqid,
                                  (const void *)&default_context, BPF_ANY);
        if (err < 0) {
            dbg("[dsq_context_store] error(%d) failed to update elem in map", err);
            return NULL;
        }

        context = bpf_map_lookup_elem(&dsq_context_store, &dsqid);
    }

    return context;
}

/*
 * Per-cgroup local storage.
 *
 * This contain all the per-cgroup information used internally by the BPF code.
 */
typedef struct cgroup_ctx {
    bool init;
    u64 task_count;

    bool is_function_cgroup;
} cgroup_ctx_t;

/* Map that contains task-local storage. */
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, MAX_MAP_ENTRIES_CMAP);
    __uint(key_size, sizeof(char) * MAX_PATH); // key: cgroup name
    __uint(value_size, sizeof(cgroup_ctx_t));  // value: context
} cgroup_ctx_stor SEC(".maps");

static cgroup_ctx_t *__noinline lookup_or_build_cgroup_ctx(const char *name, u32 max_len) {
    s32 err;

    if (name == NULL || max_len > MAX_PATH) {
        dbg("[cmap][get_cgroup_ctx][cpu_cgroup_attach] invalid args: %s %u", name, max_len);
        return NULL;
    }

    cgroup_ctx_t *cgroup_ctx = bpf_map_lookup_elem(&cgroup_ctx_stor, name);
    if (cgroup_ctx == NULL) {
        cgroup_ctx_t local_cgroup_ctx;
        memset(&local_cgroup_ctx, 0, sizeof(local_cgroup_ctx));

        err = bpf_map_update_elem(&cgroup_ctx_stor, (const void *)name,
                                  (const void *)&local_cgroup_ctx, BPF_ANY);
        if (err < 0) {
            dbg("[cmap][get_cgroup_ctx][cpu_cgroup_attach] error(%d) failed to update elem in map",
                err);
            return NULL;
        }

        cgroup_ctx = bpf_map_lookup_elem(&cgroup_ctx_stor, name);
    }

    return cgroup_ctx;
}

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, MAX_MAP_ENTRIES_CMAP);
    __uint(key_size, sizeof(pid_t));
    __uint(value_size, sizeof(char) * MAX_PATH);
} pid_cname_cache SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, MAX_MAP_ENTRIES_CMAP);
    __uint(key_size, sizeof(pid_t));
    __uint(value_size, sizeof(SchedGroupChrs_t));
} pid_chrs_cache SEC(".maps");

//////////////////////////////
// Shared Functions
#include "hashmaps.bpf.c"

// for exit error dump in user space
UEI_DEFINE(uei);

#include "cgroup_name_matching.bpf.c"
#include "utils.bpf.c"

//////////////////////////////
// Heart Beat Timer

/*
 * Heartbeat timer used to periodically trigger poll based events.
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
 * Heartbeat scheduler timer callback.
 */

static s32 usersched_timer_fn(void *map, s32 *key, struct bpf_timer *timer) {
    s32 err = 0;

    kick_filled_domains_cpus();

    /* Re-arm the timer */
    err = bpf_timer_start(timer, HEARTBEAT_INTERVAL, 0);
    if (err)
        scx_bpf_error("Failed to arm stats timer");

    return 0;
}
/*
 * Initialize the heartbeat scheduler timer.
 */
static int usersched_timer_init(void) {
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
    err = bpf_timer_start(timer, HEARTBEAT_INTERVAL, 0);
    if (err)
        scx_bpf_error("Failed to arm scheduler timer");

    return err;
}

SEC("perf_event")
int perf_sample_handler(struct bpf_perf_event_data *ctx) {
    struct bpf_perf_event_value perf_event_value;
    s32 this_cpu = bpf_get_smp_processor_id();
    int err;

    err = bpf_perf_prog_read_value(ctx, &perf_event_value, sizeof(perf_event_value));
    if (err != 0) {
        info("[perf_event] cpu: %d failed with err: %d ", bpf_get_smp_processor_id(), err);
        return err;
    }

    struct cpucycles_ctx *lcpucycles_ctx;
    lcpucycles_ctx = try_lookup_cpucycles_ctx(this_cpu);
    if (!lcpucycles_ctx) {
        return -ENOMEM;
    }

    u64 time_now = bpf_ktime_get_tai_ns();
    u64 time_elapsed = time_now - lcpucycles_ctx->last_timestamp;

    lcpucycles_ctx->cycles_counter += 1;
    if (time_elapsed > NSEC_PER_SEC) {
        lcpucycles_ctx->last_timestamp = time_now;
        lcpucycles_ctx->cycles_per_sec =
            (lcpucycles_ctx->cycles_counter * NSEC_PER_SEC) / time_elapsed;
        lcpucycles_ctx->cycles_counter = 0;
    }

    info("[perf_event] cpu: %d Mhz: %lld ", this_cpu, lcpucycles_ctx->cycles_per_sec);

    return 0;
}

//////////////////////////////
// Scx Callbacks

// dispatch the task @p to least loaded local dsq of a core that belongs
// to the domain assigned by CP
s32 BPF_STRUCT_OPS(finesched_select_cpu, struct task_struct *p, s32 prev_cpu, u64 wake_flags) {
    // all tasks are enqueued on bpf_scheduler first
    check_dsqlen_of_each_dsq_for_tracing();
    return prev_cpu;
}

// replinish the task @p timeslice
void BPF_STRUCT_OPS(finesched_enqueue, struct task_struct *p, u64 enq_flags) {

    struct task_context *tctx = try_lookup_task_ctx(p);

    if (tctx && tctx->use_specified_cpus) {
        enqueue_to_per_cpu_custom_dsq(p);
    } else if (!enqueue_to_assigned_domain_queue(p, /*to_highpriority_queue=*/true,
                                                 /*fifo=*/false)) {
        enqueue_to_global_queue(p, /*fifo=*/false);
    }
}

void BPF_STRUCT_OPS(finesched_dispatch, s32 cpu, struct task_struct *prev) {

    scx_bpf_cpuperf_set(cpu, SCX_CPUPERF_ONE);

    s32 task_count = 1; // 2 or more makes it worse across aile
    s32 tasks_leftover = task_count;
    s32 dsq_id;

    dsq_id = cpu_to_domain_highpriority_dsqid(cpu);
    tasks_leftover = tasks_leftover - move_from_custom_queue_to_local_dsq(dsq_id, tasks_leftover);
    tasks_leftover = tasks_leftover - worksteal_from_neighbors_domain_queue(
                                          /*from_highpriority_queue=*/true, cpu, tasks_leftover);

    move_from_per_cpu_custom_to_local_dsq(cpu, 1);
    move_from_custom_queue_to_local_dsq(DSQ_GLOBAL_Q_ID, 1);
}

void BPF_STRUCT_OPS(finesched_set_cpumask, struct task_struct *p, const struct cpumask *cpumask) {
    info("[info][set_cpumask][%s:%d]  updated", p->comm, p->pid);

    task_stats_task_mask_updated(p);
}

void BPF_STRUCT_OPS(finesched_running, struct task_struct *p) { task_stats_start_running(p); }
void BPF_STRUCT_OPS(finesched_stopping, struct task_struct *p, bool runnable) {
    task_stats_stop_running(p);
}
void BPF_STRUCT_OPS(finesched_quiescent, struct task_struct *p, u64 deq_flags) {
    task_stats_sleeping(p);
}

// Remember all newly created cgroups.
SEC("fentry/cpu_cgroup_attach")
s32 BPF_PROG(cpu_cgroup_attach, struct cgroup_taskset *tset) {
    char cgroup_name[MAX_PATH];
    char prefix[MAX_PATH] = "docker-";
    s32 prefix_len = 7;
    s32 err;

    memset(cgroup_name, 0, MAX_PATH);
    err = bpf_probe_read_kernel_str(cgroup_name, MAX_PATH,
                                    tset->cur_cset->dom_cset->dfl_cgrp->kn->name);
    if (err <= 0) {
        info("[cgroup_stats]  error(%d) reading cgroup_name", err);
        return 0;
    }

    if (!prefix_match(prefix, cgroup_name, /*len=*/prefix_len)) {
        return 0;
    }

    cgroup_ctx_t *cgroup_ctx = lookup_or_build_cgroup_ctx(cgroup_name, MAX_PATH);
    if (cgroup_ctx == NULL) {
        info("[cgroup_stats]  error building cgroup_ctx", err);
        return 0;
    }
    cgroup_ctx->init = true;
    cgroup_ctx->task_count = 0;

    info("[cgroup_stats]  new cgroup attached %s ", cgroup_name);
    return 0;
}

// Task @p is being created.
//    called when task is being forked
//    args has
//      fork(true: fork, false: transition path)
//      cgroup(that task is joining)
//  even tasks that don't belong to schedext class come here, but they don't
//  have scx as sched class, so they don't come into other callbacks
s32 BPF_STRUCT_OPS(finesched_init_task, struct task_struct *p, struct scx_init_task_args *args) {

    info("[init_task][%s:%d] task born", p->comm, p->pid);

    if (use_switch_kfunc) {
        switch_to_scx_if_cgroup_exists(p);
    }

    global_stats_task_init(p);
    cgroup_stats_task_init(p);
    task_stats_task_init(p);

    return 0;
}

// Task @p is exiting.
//   called when task is being exited or bpf sched is unloading
//   args has
//    cancelled(true: exiting before running on sched_ext, false: ran on
//    sched_ext do cleanup)
void BPF_STRUCT_OPS(finesched_exit_task, struct task_struct *p, struct scx_exit_task_args *args) {
    info("[exit_task] exiting task %d - %s", p->pid, p->comm);

    global_stats_task_exit(p);
    cgroup_stats_task_exit(p);
}

// CPU is entering idle state if idle is true.
void BPF_STRUCT_OPS(finesched_update_idle, s32 cpu, bool idle) {
    struct cpu_ctx *cctx = try_lookup_cpu_ctx(cpu);
    if (!cctx) {
        error("[update_idle] cctx not found for cpu %d ", cpu);
        return;
    }

    if (idle) {
        cctx->idle_start = bpf_ktime_get_ns();
    } else {
        u64 old_clk = cctx->idle_start;
        if (old_clk != 0) {
            u64 duration = bpf_ktime_get_ns() - old_clk;
            bool ret = __sync_bool_compare_and_swap(&cctx->idle_start, old_clk, 0);
            if (ret) {
                cctx->idle_time += duration;
            }
        }
    }
}

// Initialize the scheduling class.
s32 BPF_STRUCT_OPS_SLEEPABLE(finesched_init) {

    u32 i;
    u32 cpu;
    int err;

    info("[init] initializing the tsksz scheduler");

    if (use_switch_kfunc && !bpf_ksym_exists(scx_bpf_switch_to_scx)) {
        error("[init] scx_bpf_switch_to_scx is unavailable");
        return -EOPNOTSUPP;
    }

    if (enable_task_ip_monitoring && !bpf_ksym_exists(scx_bpf_task_ip)) {
        error("[init] scx_bpf_task_ip is unavailable");
        return -EOPNOTSUPP;
    }

    if (enable_timer_callback) {
        err = usersched_timer_init();
        if (err)
            return err;
    }

    err = populate_cpumasks();
    if (err < 0)
        return err;

    err = populate_cpu_to_domain_id_map();
    if (err < 0)
        return err;

    err = create_global_dsq();
    if (err < 0)
        return err;

    err = create_dsqs_per_domain();
    if (err < 0)
        return err;

    err = create_priority_dsqs_per_cpu();
    if (err < 0)
        return err;

    err = create_global_reserved_corebitmask();
    if (err < 0)
        return err;

    err = populate_cpu_to_dsq();
    if (err < 0)
        return err;

    empty_sched_chrs.id = -1;

    return 0;
}

// Unregister the scheduling class.
void BPF_STRUCT_OPS(finesched_exit, struct scx_exit_info *ei) {
    info("[exit] exiting the finesched scheduler");

    UEI_RECORD(uei, ei);
}

SCX_OPS_DEFINE(finesched_ops, .select_cpu = (void *)finesched_select_cpu,
               .enqueue = (void *)finesched_enqueue, .dispatch = (void *)finesched_dispatch,
               .set_cpumask = (void *)finesched_set_cpumask, .running = (void *)finesched_running,
               .stopping = (void *)finesched_stopping, .quiescent = (void *)finesched_quiescent,
               .update_idle = (void *)finesched_update_idle,
               .init_task = (void *)finesched_init_task, .exit_task = (void *)finesched_exit_task,
               .init = (void *)finesched_init, .exit = (void *)finesched_exit,
               .flags = SCX_OPS_KEEP_BUILTIN_IDLE | SCX_OPS_SWITCH_PARTIAL | SCX_OPS_ENQ_LAST,
               .name = "finesched");
