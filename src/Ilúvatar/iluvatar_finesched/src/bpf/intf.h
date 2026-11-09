#ifndef __INTF_H
#define __INTF_H

#include <stdbool.h>
#ifndef __kptr
#ifdef __KERNEL__
#error "__kptr_ref not defined in the kernel"
#endif
#define __kptr
#endif

#ifndef __KERNEL__
typedef int s32;
typedef long long s64;
typedef unsigned short u16;
typedef unsigned u32;
typedef unsigned long long u64;
#endif

////////////////////////////
// Misc stuff that should have been included

// defined in uapi/linux/time.h
#define CLOCK_BOOTTIME 7

////////////////////////////
// Constants and Fixed Parameters
enum consts {
    MAX_GROUPS = 48,
    MAX_MAP_ENTRIES_CMAP = 1024,
    MAX_MAP_ENTRIES_TASK_STORE = (16 * 4096),
    MAX_PATH = 100,
    // BPF arrays need a verifier-visible upper bound. The actual CPU-ID range
    // and online set are supplied by the userspace loader at runtime.
    MAX_CPUS = 1024,

    MAX_CPU_UTIL = 1000, // 100.0%

    NSEC_PER_USEC = 1000ULL,
    NSEC_PER_MSEC = 1000000ULL,
    NSEC_PER_SEC = 1000000000ULL, // Bug: It overflows the resultant when used as multiplier during
                                  // compilation.

    HEARTBEAT_INTERVAL = 1000 * NSEC_PER_MSEC,

    DSQ_INACTIVE_GRPS_N0 = 0x10,    // custom DSQ on numa node 0 - custom DSQs can
                                    // be allocated on any node, if no node is
                                    // specified - it's allocated on the node on
                                    // which the scx_bpf_create_dsq executes.
                                    // for specific cores
    DSQ_INACTIVE_GRPS_N1 = 0x11,    // for all cores
    DSQ_PRIO_GRPS_START = 0x200,    // starting id for prio dsqs for the sched
                                    // grps
    // Keep the per-CPU range disjoint from the small domain DSQ ranges even
    // when CPU IDs exceed 255.
    DSQ_PRIO_PER_CPU_START = 0x10000, // starting id for per-CPU priority DSQs

    DSQ_PRIO_Q_PER_DOM_START = 0x400,
    DSQ_REG_Q_PER_DOM_START = 0x500,
    DSQ_GLOBAL_Q_ID = 0x600,

    DSQ_MAX_COUNT = 0x20,

    INACTIVE_GRPS_TS = (10 * NSEC_PER_MSEC),

    SCHED_CONFIG_PRIO_DSQ = 0x10000,

    RESERVED_GID_SWITCH_BACK = 101,

    DEFAULT_TS = (1 * NSEC_PER_MSEC),
    MIN_TS = (1 * NSEC_PER_MSEC),

    TASK_LIFETIME_THRESHOLD = 50, // tasks older then 50*10ms -> 500ms are too
                                  // old

    TASK_ROUNDTRIPTIME_AVG_CAPTURE_THRESHOLD = 3000000000ULL, // 3secs
    TASK_ROUNDTRIPTIME_PRIO_THRESHOLD = (200 * NSEC_PER_MSEC),

    REGULAR_QUEUE_CONSUME_PERIOD_THRESHOLD = (1000 * NSEC_PER_MSEC),

    BUSYPOLLING_IP_VARIANCE_THRESHOLD = 16,
};

enum cpu_topology_flags {
    CPU_TOPOLOGY_POSSIBLE = (1U << 0),
    CPU_TOPOLOGY_ONLINE = (1U << 1),
    CPU_TOPOLOGY_BIG = (1U << 2),
    CPU_TOPOLOGY_LITTLE = (1U << 3),
    CPU_TOPOLOGY_SMT = (1U << 4),
};

typedef struct CpuTopology {
    s32 numa_node;
    s32 package_id;
    s32 die_id;
    s32 cluster_id;
    s32 core_id;
    u32 capacity;
    u32 max_freq_khz;
    u32 core_type;
    u32 flags;
    u32 smt_sibling_count;
} CpuTopology_t;

// TODO: not sure why scx_utils builder is not
// recognizing the definition from vmlinux.h during
// early build stages
#ifndef __VMLINUX_H__
struct cpumask {
    unsigned long bits[128];
};
#endif

////////////////////////////
// macros
#define debug 0
#define info(fmt, args...)                                                                         \
    do {                                                                                           \
        if (debug)                                                                                 \
            bpf_printk(fmt, ##args);                                                               \
    } while (0)
#define error(fmt, args...)                                                                        \
    do {                                                                                           \
        bpf_printk("[error]" #fmt, ##args);                                                        \
    } while (0)
#define dbg(fmt, args...)                                                                          \
    do {                                                                                           \
        if (debug)                                                                                 \
            bpf_printk(fmt, ##args);                                                               \
    } while (0)
#define trace(fmt, args...)                                                                        \
    do {                                                                                           \
        if (debug > 1)                                                                             \
            bpf_printk(fmt, ##args);                                                               \
    } while (0)

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))

////////////////////////////
// Group Related Definitions
typedef s32 SchedGroupID;

enum QEnqPrioType {
    QEnqPrioUndef = 0,
    QEnqPrioArrival,
    QEnqPrioSRPTover,
    QEnqPrioSRPTreset,
    QEnqPrioSHRTDUR,
    QEnqPrioSHRTDURUW,
    QEnqPrioINVOC,
    QEnqPrioPLAIN,
    QEnqPrioTaskCount,
};

// Group Statistics Structure
typedef struct SchedGroupStats {
    u64 util;
    u64 avg_util;
    u64 dsqlen;
    u64 avg_freq_mhz;
} SchedGroupStats_t;

// Per-logical-CPU scheduler metrics exported to userspace. Utilization values
// use MAX_CPU_UTIL as 100%; queue lengths are sampled when sched_ext callbacks
// run on the CPU.
typedef struct CoreSchedMetrics {
    u64 local_dsq_len;
    u64 domain_dsq_len;
    u64 runnable;
    u64 util;
    u64 avg_util;
} CoreSchedMetrics_t;

// Group Characteristics Structure
typedef struct SchedGroupChrs {
    SchedGroupID id;
    struct cpumask reserved_corebitmask; // for high priority tasks
    struct cpumask corebitmask;          // for regular tasks
    u64 core_count;
    u64 timeslice; // ms
    u32 fifo;      // single queue should be fifo or not
    u32 prio;      // enqueue priority type:
    u32 perf;      // frequency setting

} SchedGroupChrs_t;

////////////////////////////
// cgroup Related Definitions
typedef struct CgroupStatus {
    char cur_cgroup_prefix[MAX_PATH]; // cgroup prefix of the
} CgroupStatus_t;

typedef struct CgroupChrs {
    SchedGroupID gid;
    u64 invoke_ts;  //  invocation
    u64 arrival_ts; //  arrival
    u64 workerdur;  //  execution time
} CgroupChrs_t;

#endif // __INTF_H
