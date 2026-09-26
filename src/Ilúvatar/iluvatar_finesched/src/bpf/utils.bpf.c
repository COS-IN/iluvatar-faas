#ifndef __UTILS_F
#define __UTILS_F

char DOCKER_CGROUP_PREFIX[MAX_PATH] = "docker/";

////////////////////////////
// Custom Kfuncs Declarations
u64 scx_bpf_task_ip(struct task_struct *p) __ksym __weak;
void scx_bpf_switch_to_scx(struct task_struct *p) __ksym __weak;
void scx_bpf_switch_to_normal(struct task_struct *p) __ksym __weak;
u32 bpf_cpumask_first_and(const struct cpumask *src1, const struct cpumask *src2) __ksym;

////////////////////////////
// BitMask Helpers
/*
 * Allocate/re-allocate a new cpumask.
 * Thanks to andrea from bpfland code.
 */
static s32 calloc_cpumask(struct bpf_cpumask **p_cpumask) {
    struct bpf_cpumask *cpumask;

    cpumask = bpf_cpumask_create();
    if (!cpumask)
        return -ENOMEM;

    cpumask = bpf_kptr_xchg(p_cpumask, cpumask);
    if (cpumask)
        bpf_cpumask_release(cpumask);

    return 0;
}

static void dump_cpumask(struct cpumask *bitmask) {
    s32 cpu;
    char buf[128] = "", *p;

    if (!bitmask) {
        return;
    }

    p = buf;
    bpf_for(cpu, 0, MAX_CPUS) {

        if (!(p = MEMBER_VPTR(buf, [p - buf])))
            break;

        if (bpf_cpumask_test_cpu(cpu, bitmask))
            *p++ = '0' + cpu % 10;
        else
            *p++ = '.';

        if ((cpu & 7) == 7) {
            if (!(p = MEMBER_VPTR(buf, [p - buf])))
                break;

            *p++ = '|';
        }
    }
    buf[sizeof(buf) - 1] = '\0';

    info("[get_sched_cpu_ts] bitmask %s", buf);
}

#define POPULATE_CPUMASK(mask, core_array)                                                         \
    do {                                                                                           \
        err = calloc_cpumask(&mask);                                                               \
        if (err)                                                                                   \
            return err;                                                                            \
        bpf_rcu_read_lock();                                                                       \
        bpf_for(i, 0, sizeof(core_array) / sizeof(u8)) {                                           \
            if (mask) {                                                                            \
                bpf_cpumask_set_cpu(core_array[i], mask);                                          \
            }                                                                                      \
        }                                                                                          \
        bpf_rcu_read_unlock();                                                                     \
    } while (0)

static __noinline int populate_cpumasks() {
    int err;
    int i;

    // Populate mask for Inactive Groups
    POPULATE_CPUMASK(cores_inact_grp_mask, cores_inact_grp);

    // Populate mask for NUMA Nodes
    POPULATE_CPUMASK(cpumask_node0, cores_node0);
    POPULATE_CPUMASK(cpumask_node1, cores_node1);

    return 0;
}

static __noinline s32 cpu_to_numanode(s32 cpu) { return cpu < (MAX_CPUS / 2) ? 0 : 1; }

static __noinline int cpumask_to_numanode(struct bpf_cpumask *cpumask) {

    int r = -1;

    bpf_rcu_read_lock();

    if (!cpumask) {
        return -1;
    }

    if (cpumask_node0 && bpf_cpumask_intersects(cpumask, cpumask_node0)) {
        r = 0;
    } else if (cpumask_node1 && bpf_cpumask_intersects(cpumask, cpumask_node1)) {
        r = 1;
    }
    bpf_rcu_read_unlock();
    return r;
}

static __noinline long callback_gmap_populate_cpu_to_dsq_iter(struct bpf_map *map, const void *key,
                                                              void *val, void *ctx) {

    int err;

    if (!key || !val) {
        return 1;
    }

    SchedGroupID *gid = (SchedGroupID *)key;
    SchedGroupChrs_t *chrs = (SchedGroupChrs_t *)val;

    int dsqid = DSQ_PRIO_GRPS_START + *gid;
    struct cpu_ctx *cctx;
    s32 cpu;
    bpf_for(cpu, 0, MAX_CPUS) {
        if (bpf_cpumask_test_cpu(cpu, &chrs->corebitmask)) {
            cctx = try_lookup_cpu_ctx(cpu);
            if (!cctx) {
                error("[dsqs][gmap] cctx not found for cpu %d ", cpu);
                return 1;
            }
            cctx->prio_dsqid = dsqid;
            cctx->last_vtime = bpf_ktime_get_ns();
            info("[dsqs][gmap] dsq %d set in context of cpu %d for gid %d", dsqid, cpu, *gid);
        }
    }

    return 0;
}

static __noinline s32 populate_cpu_to_dsq() {

    int count;
    count = bpf_for_each_map_elem(&gMap, &callback_gmap_populate_cpu_to_dsq_iter, &count, 0);
    info("[dsqs][gmap] iterated total of %d elements to populate cpu to dsq map", count);

    return 0;
}

static __noinline struct bpf_cpumask *cpu_mask_intersection(struct cpumask *mask0,
                                                            struct cpumask *mask1) {
    struct bpf_cpumask *cpumask = bpf_cpumask_create();
    if (!cpumask)
        return NULL;

    bpf_cpumask_and(cpumask, mask0, mask1);
    return cpumask;
}

//////
// CPU helpers

#define KICK_ALL_CPUS_CALL_COUNT_THRESHOLD (MAX_CPUS / 4)
static s32 kick_all_cpus_call_count = 0;

static __always_inline void kick_cpus(struct cpumask *mask) {
    s32 cpu;
    bpf_for(cpu, 0, MAX_CPUS) {
        if (bpf_cpumask_test_cpu(cpu, mask)) {
            scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE);
        }
    }
}

static __always_inline void kick_all_cpus() {
    s32 cpu;
    bpf_for(cpu, 0, MAX_CPUS) { scx_bpf_kick_cpu(cpu, SCX_KICK_IDLE); }
}

// kicks all cpus only when number of calls exceed
// a threshold
static __noinline void kick_all_cpus_every_nth_call() {
    kick_all_cpus_call_count += 1;
    if (kick_all_cpus_call_count < KICK_ALL_CPUS_CALL_COUNT_THRESHOLD) {
        return;
    }
    kick_all_cpus_call_count = 0;

    kick_all_cpus();
}

static long __noinline callback_gmap_kick_cpus_iter(struct bpf_map *map, const void *key, void *val,
                                                    void *ctx) {
    if (!key || !val) {
        return 1;
    }

    SchedGroupID *gid = (SchedGroupID *)key;
    SchedGroupChrs_t *chrs = (SchedGroupChrs_t *)val;

    s32 len;
    len = scx_bpf_dsq_nr_queued(DSQ_PRIO_Q_PER_DOM_START + *gid);
    len += scx_bpf_dsq_nr_queued(DSQ_REG_Q_PER_DOM_START + *gid);
    if (len > 0) {
        kick_cpus(&chrs->corebitmask);
    }

    return 0;
}

static void __noinline kick_filled_domains_cpus() {
    int count;
    count = bpf_for_each_map_elem(&gMap, &callback_gmap_kick_cpus_iter, &count, 0);
    info("[kick_cpus][gmap] %d domains in gmap", count);
}

static void __noinline boost_cpus() {
    u32 cpu;
    u32 cpu_perf_lvl;

    bpf_for(cpu, 0, MAX_CPUS) {
        cpu_perf_lvl = scx_bpf_cpuperf_cur(cpu);
        info("[dsqs][cpufreq] cpu: %d cur perf lvl: %d", cpu, cpu_perf_lvl);
        scx_bpf_cpuperf_set(cpu, SCX_CPUPERF_ONE);
    }
}
////////////////////////////
// Map Lookup

static SchedGroupChrs_t *__noinline get_schedgroup_chrs(SchedGroupID gid) {

    if (gid > MAX_GROUPS) {
        goto out_not_found;
    }

    SchedGroupChrs_t *sched_chrs = bpf_map_lookup_elem(&gMap, &gid);
    if (!sched_chrs) {
        goto out_not_found;
    }

    return sched_chrs;

out_not_found:
    dbg("[warn][gmap][get_schedgroup_chrs] not found for gid: %u", gid);
    return NULL;
}

static CgroupChrs_t *__noinline get_cgroup_chrs(const char *name, u32 max_len) {
    if (!name || max_len > MAX_PATH) {
        dbg("[cmap][get_cgroup_chrs] invalid args: %s %u", name, max_len);
        return NULL;
    }

    CgroupChrs_t *cgrp_chrs;
    u32 cpu = bpf_get_smp_processor_id();

    cgrp_chrs = bpf_map_lookup_elem(&cMap, name);
    if (!cgrp_chrs) {
        dbg("[cmap][get_cgroup_chrs] cgroup %s not found in cMap ", name);
        cgrp_chrs = bpf_map_lookup_percpu_elem(&cMapLast, name, cpu);
        if (!cgrp_chrs) {
            dbg("[cmap][get_cgroup_chrs] cgroup %s not found in cMapLast ", name);
        }
    } else {
        dbg("[cmap][get_cgroup_chrs] cgroup %s is found in cMap ", name);
        bpf_map_update_elem(&cMapLast, (const void *)name, cgrp_chrs, BPF_ANY);
    }

    return cgrp_chrs;
}

static SchedGroupStats_t *__noinline get_schedgroup_stats(SchedGroupID gid) {
    if (gid > MAX_GROUPS) {
        goto out_not_found;
    }

    SchedGroupStats_t lsched_stats;
    SchedGroupStats_t *sched_stats = bpf_map_lookup_elem(&gStats, &gid);
    if (!sched_stats) {
        memset(&lsched_stats, 0, sizeof(SchedGroupStats_t));
        bpf_map_update_elem(&gStats, (const void *)&gid, &lsched_stats, BPF_ANY);
    }
    sched_stats = bpf_map_lookup_elem(&gStats, &gid);
    if (!sched_stats) {
        goto out_not_found;
    }

    return sched_stats;

out_not_found:
    dbg("[warn][gmap][get_schedgroup_stats] not found for gid: %u", gid);
    return NULL;
}

////////////////////////////
// Map Dumping Functions
static long callback_print_cMap_element(struct bpf_map *map, char *cgroup_name, CgroupChrs_t *val,
                                        void *data) {
    if (cgroup_name == NULL) {
        return 1;
    }
    if (val == NULL) {
        return 1;
    }
    dbg("[finesched][cMap] cgroup_name: %s gid: %d", cgroup_name, val->gid);
    return 0;
}

static long callback_print_gMap_element(struct bpf_map *map, SchedGroupID *gid,
                                        SchedGroupChrs_t *val, void *data) {
    if (gid == NULL) {
        return 1;
    }
    if (val == NULL) {
        return 1;
    }
    dbg("[finesched][gMap][bitmask] gid: %llu ts: %lu", *gid, val->timeslice);
    dump_cpumask(&val->corebitmask);
    return 0;
}

static void __noinline dump_gMap() {
    // TODO: use bpf_for_each_map_elem(...)
    s32 key;
    bpf_for(key, 0, MAX_GROUPS) {
        SchedGroupChrs_t *val = bpf_map_lookup_elem(&gMap, (const void *)&key);
        callback_print_gMap_element(NULL, &key, val, NULL);
    }
}

//////
// Task -> SchedGroupChrs_t, CgroupChrs_t, SchedGroupID

static bool __noinline is_docker_schedcgroup(struct task_struct *p) {

    char *cgrp_path;
    cgrp_path = get_schedcgroup_path(p);
    if (!cgrp_path) {
        return false;
    }

    return match_prefix(DOCKER_CGROUP_PREFIX, cgrp_path, MAX_PATH);
}

static __noinline SchedGroupChrs_t * get_cgroup_chrs_for_p(struct task_struct *p) {

    char *cg_name = get_schedcgroup_name(p);
    if (cg_name == NULL) {
        goto out_bad_name;
    }
    CgroupChrs_t *cgrp_chrs = get_cgroup_chrs(cg_name, MAX_PATH);
    if (cgrp_chrs == NULL) {
        goto out_no_chrs;
    }
    return cgrp_chrs;

out_bad_name:
    info("[warn][cgroup_chrs] no cg_name found for task %d - %s", p->pid, p->comm);
    return NULL;
out_no_chrs:
    info("[warn][cgroup_chrs] no cgroup_chrs found for task %d - %s cgname: %s", p->pid, p->comm,
         cg_name);
    return NULL;
}


static __noinline u64 task_cgroup_exec_time_weight(struct task_struct *p) {
  u64 weight_upper_bound = 500000000; // delay in ns 

  CgroupChrs_t *cgrp_chrs = get_cgroup_chrs_for_p(p);
  if (!cgrp_chrs) {
    return weight_upper_bound;
  }

  info("[task_stats][%s:%d] p->cgroup->workerdur %llu ", p->comm, p->pid,
       cgrp_chrs->workerdur);
  
  u64 func_exec_time_ms = cgrp_chrs->workerdur;
  u64 upper_threshold_ms = 5000; 
  func_exec_time_ms = MIN( func_exec_time_ms, upper_threshold_ms );
  
  return (func_exec_time_ms*weight_upper_bound)/upper_threshold_ms + 1;
}

static __noinline u64 task_cgroup_exec_time(struct task_struct *p) {
  CgroupChrs_t *cgrp_chrs = get_cgroup_chrs_for_p(p);
  if (!cgrp_chrs) {
    return 1;
  }

  info("[task_stats][%s:%d] p->cgroup->workerdur %llu ", p->comm, p->pid,
       cgrp_chrs->workerdur);
  
  return cgrp_chrs->workerdur + 1;
}

static cgroup_ctx_t *__noinline get_cgroup_ctx_for_p(struct task_struct *p) {

    char *cg_name = get_schedcgroup_name(p);
    if (cg_name == NULL) {
        goto out_no_ctx;
    }
    cgroup_ctx_t *cgrp_ctx = lookup_or_build_cgroup_ctx(cg_name, MAX_PATH);
    if (cgrp_ctx == NULL) {
        goto out_no_ctx;
    }
    return cgrp_ctx;

out_no_ctx:
    info("[warn][cgroup_ctx] no cgroup_ctx found for task %d - %s", p->pid, p->comm);
    return NULL;
}

static SchedGroupChrs_t *__noinline get_schedchrs(struct task_struct *p) {

    CgroupChrs_t *cgrp_chrs = get_cgroup_chrs_for_p(p);
    if (cgrp_chrs == NULL) {
        goto out_no_chrs;
    }

    SchedGroupChrs_t *sched_chrs = get_schedgroup_chrs(cgrp_chrs->gid);
    if (sched_chrs == NULL) {
        goto out_no_chrs;
    }

    return sched_chrs;

out_no_chrs:
    info("[warn][schedgroup] no schedcgroup chrs found for task %d - %s", p->pid, p->comm);
    return NULL;
}

static SchedGroupChrs_t *__noinline get_schedchrs_cached(struct task_struct *p) {

    // return get_schedchrs( p );

    pid_t pid = p->pid;
    SchedGroupChrs_t *chrs = bpf_map_lookup_elem(&pid_chrs_cache, (const void *)&pid);
    if (chrs) {
        if (chrs->id == -1) {
            return NULL;
        }
    }
    return chrs;
}

static void __noinline update_caches(struct task_struct *p) {

    pid_t pid = p->pid;

    char *cname;
    cname = get_schedcgroup_name(p);
    if (!cname) {
        return;
    }
    bpf_map_update_elem(&pid_cname_cache, (const void *)&pid, cname, BPF_ANY);
    info("[caches] cached pid %d - cgroup %s ", pid, cname);

    // in case cMap has -1 gid - it would return NULL
    SchedGroupChrs_t *chrs = get_schedchrs(p);
    if (!chrs) {
        chrs = &empty_sched_chrs;
    }

    bpf_map_update_elem(&pid_chrs_cache, (const void *)&pid, chrs, BPF_ANY);
    info("[caches] cached pid %d - gid %d ", pid, chrs->id);
    // dump_cpumask(&chrs->corebitmask);
}

static long __noinline callback_pid_chrs_cache_iter(struct bpf_map *map, const void *key, void *val,
                                                    void *ctx) {

    if (!key || !val) {
        return 1;
    }

    const pid_t *pid = (const pid_t *)key;
    SchedGroupChrs_t *chrs = (SchedGroupChrs_t *)val;

    info("[pid_chrs_cache] iterated pid %d - chrs for gid %d ", *pid, chrs->id);
    struct task_struct *p = bpf_task_from_pid(*pid);
    if (p) {
        update_caches(p);
        bpf_task_release(p);
    }

    return 0;
}

static void __noinline poll_update_pid_gid_cache() {

    int count;
    count = bpf_for_each_map_elem(&pid_chrs_cache, &callback_pid_chrs_cache_iter, &count, 0);
    info("[pid_chrs_cache] iterated total of %d elements", count);
}

////////
// stats capturing

// Credits to Changwoo author of scx_lavd
static u64 calc_avg(u64 old_val, u64 new_val) {
    /*
     * Calculate the exponential weighted moving average (EWMA).
     *  - EWMA = (0.75 * old) + (0.25 * new)
     */
    return (old_val - (old_val >> 2)) + (new_val >> 2);
}

static __noinline void task_stats_task_roundtrip_since_last_wakeup(struct task_struct *p) {
    struct task_context *tctx;
    tctx = try_lookup_task_ctx(p);

    if (p && tctx) {
        u64 last_wakeup_time = tctx->last_wakeup_time;
        u64 current_time = bpf_ktime_get_tai_ns();

        // corner case for the starting point
        last_wakeup_time = last_wakeup_time == 0 ? current_time : last_wakeup_time;

        last_wakeup_time = current_time > last_wakeup_time ? last_wakeup_time : current_time - 1;
        u64 roundtrip_time = current_time - last_wakeup_time;
        tctx->last_wakeup_time = current_time;

        if (roundtrip_time != 1 && roundtrip_time < TASK_ROUNDTRIPTIME_AVG_CAPTURE_THRESHOLD) {
            // corner case for the starting point
            tctx->wakeup_roundtrip_time_avg = tctx->wakeup_roundtrip_time_avg == 0
                                                  ? roundtrip_time
                                                  : tctx->wakeup_roundtrip_time_avg;

            tctx->wakeup_roundtrip_time_avg =
                calc_avg(tctx->wakeup_roundtrip_time_avg, roundtrip_time);
        }
    }
}

static __noinline u64 task_stats_task_ip_variance(struct task_struct *p) {
    if (!p || !bpf_ksym_exists(scx_bpf_task_ip)) {
        return 0;
    }

    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return 0;
    }

    u64 task_ip = scx_bpf_task_ip(p);
    u64 sample_count = tctx->ip_sample_count + 1;

    u64 mean_prev = tctx->ip_mean;
    u64 mean = ((tctx->ip_mean*tctx->ip_sample_count)+task_ip) / sample_count;
    
    u64 squares_update = (s64)(((s64)task_ip - (s64)mean)*((s64)task_ip - (s64)mean_prev));
    u64 variance = ((tctx->ip_variance*tctx->ip_sample_count)+squares_update) / sample_count;

    tctx->ip_mean = mean;
    tctx->ip_variance = variance;
    tctx->ip_sample_count = sample_count > 1 ? 1 : sample_count;

    info("[task_stats][%s:%d] eip(0x%llx) mean(0x%llx) variance(0x%llx) sample_count(0x%llx)", p->comm, p->pid, task_ip, mean, variance, sample_count);

    return variance;
}

static __noinline void task_stats_task_wokenup(struct task_struct *p) {
    if (!p) {
        return;
    }

    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return;
    }

    info("[task_stats][%s:%d] woken up tctx->vtime %llu ", p->comm, p->pid, tctx->vtime);
}

static __noinline void task_stats_task_enqueued_reset(struct task_struct *p) {
    struct task_context *tctx;
    tctx = try_lookup_task_ctx(p);

    if (p && tctx) {
        tctx->enqueue_count = 0;
        info("[timeslice_shortening][%s:%d] tctx->enqueue_count %llu ", p->comm, p->pid,
             tctx->enqueue_count);
    }
}

static __noinline void task_stats_task_enqueued(struct task_struct *p) {
    struct task_context *tctx;
    tctx = try_lookup_task_ctx(p);

    if (p && tctx) {
        tctx->enqueue_count = tctx->enqueue_count + 1;
        info("[timeslice_shortening][%s:%d] tctx->enqueue_count %llu ", p->comm, p->pid,
             tctx->enqueue_count);
    }
}

static __noinline void task_stats_start_running(struct task_struct *p) {
    struct task_context *tctx;
    tctx = try_lookup_task_ctx(p);

    if (tctx) {
        tctx->running_start_time = bpf_ktime_get_tai_ns();
    }
}

static __noinline void task_stats_stop_running(struct task_struct *p) {
    if (!p) {
        return;
    }

    struct task_context *tctx;
    tctx = try_lookup_task_ctx(p);

    if (tctx) {
        u64 current_time = bpf_ktime_get_tai_ns();
        u64 start_time = tctx->running_start_time;
        start_time = current_time > start_time ? start_time : current_time - 1;

        u64 cpu_time = current_time - start_time;
        if (cpu_time != 1) {
            u64 sleep_freq_avg = tctx->sleep_freq_avg;
            sleep_freq_avg = sleep_freq_avg == 0 ? 1 : sleep_freq_avg;

            // prioritize tasks that consume less cpu time
            // and sleep often
            tctx->vtime += (cpu_time*tctx->busypolling_factor) / sleep_freq_avg;

            // prioritize tasks of shorter functions 
            // u64 func_exec_time = task_cgroup_exec_time( p );
            // tctx->vtime += cpu_time * func_exec_time; 


            // every nth enqueue reset to current time to
            // preserve fairness
            // long task ~1ms
            // short task ~100us
            // n = 10
            // 100*10 = 1000
            if ((tctx->enqueue_count % 10) == 0) {
                tctx->vtime = bpf_ktime_get_tai_ns();
            }

            tctx->cpu_time_avg = calc_avg(tctx->cpu_time_avg, cpu_time);
            info("[task_stats][%s:%d] tctx->cpu_time_avg %llu ", p->comm, p->pid,
                 tctx->cpu_time_avg);
        }
        

        if (enable_task_ip_monitoring) {
            u64 ip_variance = task_stats_task_ip_variance(p);
            tctx->busypolling_factor = ip_variance > BUSYPOLLING_IP_VARIANCE_THRESHOLD ? 1 : 3;
        }
    }
}

static __noinline void task_stats_sleeping(struct task_struct *p) {
    if (!p) {
        return;
    }

    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return;
    }

    // sleep counter
    tctx->sleeps_count += 1;

    // update avg freq each second
    u64 current_time = bpf_ktime_get_tai_ns();
    u64 last_sleep_freq_update_time = tctx->last_sleep_freq_update_time; // starts
                                                                         // from
                                                                         // zero
    last_sleep_freq_update_time =
        current_time > last_sleep_freq_update_time ? last_sleep_freq_update_time : current_time - 1;

    u64 time_elapsed = current_time - last_sleep_freq_update_time;
    u64 ns_in_one_sec = 1000 * NSEC_PER_MSEC;
    if (time_elapsed < ns_in_one_sec) {
        return;
    }
    time_elapsed = time_elapsed / ns_in_one_sec;

    u64 sleep_freq = tctx->sleeps_count / time_elapsed;
    sleep_freq = sleep_freq == 0 ? 1 : sleep_freq;

    u64 sleep_freq_avg = tctx->sleep_freq_avg;
    sleep_freq_avg = sleep_freq_avg == 0 ? sleep_freq : sleep_freq_avg;

    tctx->sleep_freq_avg = calc_avg(sleep_freq_avg, sleep_freq);
    tctx->last_sleep_freq_update_time = current_time;
    tctx->sleeps_count = 0;

    info("[task_stats][%s:%d] tctx->sleep_freq_avg %llu ", p->comm, p->pid, tctx->sleep_freq_avg);
}

static __noinline void task_stats_task_init(struct task_struct *p) {
    if (!p) {
        return;
    }

    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return;
    }

    // vtime is current time which is updated on
    // every nth enqueue
    // for n-1 enqueues tasks that have less cpu time
    // and sleep frequently are prioritized
    // it preserves fairness and prioritizes short tasks
    tctx->vtime = bpf_ktime_get_tai_ns();

    char *name = get_schedcgroup_name(p);
    if (!name) {
        return;
    }

    cgroup_ctx_t *cgroup_ctx = bpf_map_lookup_elem(&cgroup_ctx_stor, name);
    if (!cgroup_ctx) {
        return;
    }

    if (cgroup_ctx->is_function_cgroup) {
        tctx->is_worker = true;
    }

    info("[task_stats][%s:%d] tctx->is_worker %d  cgroup_ctx->task_count %d ", p->comm, p->pid,
         tctx->is_worker, cgroup_ctx->task_count);
}

static __noinline void task_stats_task_mask_updated(struct task_struct *p) {
    if (!p) {
        return;
    }

    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return;
    }

    u32 number_of_set_cpus = bpf_cpumask_weight(p->cpus_ptr);
    tctx->use_specified_cpus = number_of_set_cpus <= MAX_CPUS / 2;
}

static __always_inline bool prefix_match(char *prefix, char *string, s32 len) {
    if (!prefix || !string) {
        return false;
    }
    len = len > MAX_PATH ? MAX_PATH : len;

    char left, right;
    s32 i;
    bpf_for(i, 0, len) {

        left = prefix[i];
        right = string[i];

        if (!left || !right || (left != right)) {
            return false;
        }
    }

    return true;
}

static __noinline bool is_gunicorn_task(struct task_struct *p) {
    if (!p->comm) {
        return false;
    }

#define MAX_COMM_LEN 0x7f
    char name[MAX_COMM_LEN] = "gunicorn";
    char name_len = 8;
    char comm[MAX_COMM_LEN] = "";

    bpf_probe_read_kernel_str(comm, MAX_COMM_LEN, p->comm);

    return prefix_match(name, comm, name_len);
}

static __noinline void cgroup_stats_task_init(struct task_struct *p) {
    if (!p) {
        return;
    }

    char *name = get_schedcgroup_name(p);
    if (!name) {
        return;
    }

    cgroup_ctx_t *cgroup_ctx = bpf_map_lookup_elem(&cgroup_ctx_stor, name);
    if (!cgroup_ctx) {
        return;
    }

    if (is_gunicorn_task(p)) {
        cgroup_ctx->is_function_cgroup = true;
    }

    cgroup_ctx->task_count += 1;
    info("[cgroup_stats][%s:%d] task_init cgroup_ctx->task_count %llu ", p->comm, p->pid,
         cgroup_ctx->task_count);
}

static __noinline void cgroup_stats_task_exit(struct task_struct *p) {
    if (!p) {
        return;
    }

    char *name = get_schedcgroup_name(p);
    if (!name) {
        return;
    }

    cgroup_ctx_t *cgroup_ctx = bpf_map_lookup_elem(&cgroup_ctx_stor, name);
    if (!cgroup_ctx) {
        return;
    }

    cgroup_ctx->task_count -= 1;
    info("[cgroup_stats][%s:%d] task_exit cgroup_ctx->task_count %llu ", p->comm, p->pid,
         cgroup_ctx->task_count);
}

static __noinline void global_stats_task_init(struct task_struct *p) {
    if (!p) {
        return;
    }

    global_stats.task_count += 1;
    info("[global_stats][%s:%d] task_init global_stats.task_count %llu ", p->comm, p->pid,
         global_stats.task_count);
}

static __noinline void global_stats_task_exit(struct task_struct *p) {
    if (!p) {
        return;
    }

    global_stats.task_count -= 1;
    info("[global_stats][%s:%d] task_exit global_stats.task_count %llu ", p->comm, p->pid,
         global_stats.task_count);
}

static __noinline void dsq_stats_task_consumed(u64 dsqid) {
    struct dsq_context *dsq_context = try_lookup_dsq_context(dsqid);
    if (!dsq_context) {
        return;
    }

    dsq_context->last_consume_time = bpf_ktime_get_tai_ns();
}

static void __always_inline stats_update_percpu_util(struct cpu_ctx *c) {
    u64 now = bpf_ktime_get_ns();
    if (!c) {
        return;
    }

    // update idle time
    u64 old_clk = c->idle_start;
    if (old_clk != 0) {
        bool ret = __sync_bool_compare_and_swap(&c->idle_start, old_clk, now);
        if (ret) {
            c->idle_time += now - old_clk;
        }
    }

    u64 idle_dur = c->idle_time - c->prev_idle_time;
    c->prev_idle_time = c->idle_time;

    u64 wclk = now - c->last_calc_time;
    c->last_calc_time = now;

    u64 compute = wclk - idle_dur;
    u64 util;
    util = (compute * MAX_CPU_UTIL) / wclk;
    if (util > MAX_CPU_UTIL) {
        // drop bad reading it happens during init only
        return;
    }
    c->util = util;
    c->avg_util = calc_avg(c->avg_util, c->util);
}

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(u32));
    /* double size because verifier can't follow length calculation */
    __uint(value_size, sizeof(u64) * DSQ_MAX_COUNT);
    __uint(max_entries, 1);
} dom_util_bufs SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(u32));
    /* double size because verifier can't follow length calculation */
    __uint(value_size, sizeof(u64) * DSQ_MAX_COUNT);
    __uint(max_entries, 1);
} dom_autil_bufs SEC(".maps");

__always_inline void *get_dom_buf(struct bpf_map *map) {
    u32 zero = 0;
    void *buff = bpf_map_lookup_elem(map, &zero);
    return buff;
}

static void __noinline stats_update_global() {
    s32 cpu;
    u64 dsqid = 0;
    u64 util = 0;
    u64 autil = 0;

    u64 *dom_util = get_dom_buf(&dom_util_bufs);
    u64 *dom_autil = get_dom_buf(&dom_autil_bufs);
    if (!dom_util || !dom_autil) {
        return;
    }

    SchedGroupID gid = 0;
    struct cpu_ctx *cctx = NULL;
    SchedGroupStats_t *stats = NULL;
    SchedGroupChrs_t *chrs = NULL;

    bpf_for(cpu, 0, MAX_CPUS) {
        // TODO: it causes contention across cores! every 200ms
        // maybe use a lockless structure to accumulate stuff
        // that would have to write to shared memory on each call
        // this is just asking for every 200ms
        // trace dump is delayed because of it but the numbers are
        // still every 200ms

        cctx = try_lookup_cpu_ctx(cpu);
        if (cctx) {
            stats_update_percpu_util(cctx);
            util += cctx->util;
            autil += cctx->avg_util;

            dsqid = cctx->prio_dsqid;
            if (dsqid != 0) {
                gid = dsqid - DSQ_PRIO_GRPS_START;
            }
            if (0 <= gid && gid < DSQ_MAX_COUNT) {
                dom_util[gid] += cctx->util;
                dom_autil[gid] += cctx->avg_util;
            }
        }
    }

    bpf_for(gid, 0, DSQ_MAX_COUNT) {
        stats = get_schedgroup_stats(gid);
        chrs = get_schedgroup_chrs(gid);
        if (chrs && stats) {
            dom_util[gid] /= chrs->core_count;
            stats->util = dom_util[gid];

            dom_autil[gid] /= chrs->core_count;
            stats->avg_util = dom_autil[gid];

            info("[stats][dev] found stats for gid: %llu util: %llu autil: %llu core_count %llu",
                 gid, stats->util, stats->avg_util, chrs->core_count);
        }
    }
    util /= 48;
    autil /= 48;
    info("[stats][cpu] across 48 cores util: %llu avg util: %llu", util, autil);
}

////////////////////////////
// DSQ helpers
//   it's possible to iterate dsqs see in ext.c kernel
//     bpf_iter_scx_dsq_new
//     bpf_iter_scx_dsq_next
//     bpf_iter_scx_dsq_destroy

static __noinline long callback_gmap_create_dsqs_iter(struct bpf_map *map, const void *key,
                                                      void *val, void *ctx) {

    int err;

    if (!key || !val) {
        return 1;
    }

    SchedGroupID *gid = (SchedGroupID *)key;
    SchedGroupChrs_t *chrs = (SchedGroupChrs_t *)val;

    int numa_node = cpumask_to_numanode(&chrs->corebitmask);
    if (numa_node < 0) {
        error("[dsqs][gmap] numa node of gid %d is incorrectly identified", *gid);
        return 1;
    }

    int dsqid;
    dsqid = DSQ_PRIO_Q_PER_DOM_START + *gid;
    err = scx_bpf_create_dsq(dsqid, numa_node);
    if (err < 0)
        return 1;

    dsqid = DSQ_REG_Q_PER_DOM_START + *gid;
    err = scx_bpf_create_dsq(dsqid, numa_node);
    if (err < 0)
        return 1;

    return 0;
}

static s32 __noinline create_dsqs_per_domain() {

    s32 count;
    count = bpf_for_each_map_elem(&gMap, &callback_gmap_create_dsqs_iter, &count, 0);
    info("[dsqs][gmap] iterated total of %d elements to create priority dsqs", count);
    domains_count = count;

    return 0;
}

static __always_inline s32 clip_dsqid_to_bounds(u64 dsqid, u64 starting_offset) {
    dsqid = dsqid < starting_offset ? starting_offset : dsqid;
    dsqid =
        dsqid >= (starting_offset + domains_count) ? (starting_offset + domains_count) - 1 : dsqid;
    return dsqid;
}

static s32 __noinline create_global_dsq() {
    s32 dsqid;

    dsqid = DSQ_GLOBAL_Q_ID + 0;
    return scx_bpf_create_dsq(dsqid, 0);
}

static u64 cpu_to_domain_id[MAX_CPUS] = {0};

static __noinline long callback_gmap_populate_cpu_to_domain_id_map_iter(struct bpf_map *map,
                                                                        const void *key, void *val,
                                                                        void *ctx) {

    if (!key || !val) {
        return 1;
    }

    SchedGroupID *gid = (SchedGroupID *)key;
    SchedGroupChrs_t *chrs = (SchedGroupChrs_t *)val;

    s32 cpu;
    bpf_for(cpu, 0, MAX_CPUS) {
        if (bpf_cpumask_test_cpu(cpu, &chrs->corebitmask)) {
            cpu_to_domain_id[cpu] = *gid;
        }
    }

    return 0;
}

static s32 __noinline populate_cpu_to_domain_id_map() {
    s32 count;
    count =
        bpf_for_each_map_elem(&gMap, &callback_gmap_populate_cpu_to_domain_id_map_iter, &count, 0);

    return 0;
}

static __always_inline s32 clip_cpu_to_bounds(s32 cpu) {
    cpu = cpu < 0 ? 0 : cpu;
    cpu = cpu >= MAX_CPUS ? MAX_CPUS - 1 : cpu;
    return cpu;
}

static u64 __noinline cpu_to_domain_highpriority_dsqid(s32 cpu) {
    cpu = clip_cpu_to_bounds(cpu);
    return cpu_to_domain_id[cpu] + DSQ_PRIO_Q_PER_DOM_START;
}

static u64 __noinline cpu_to_domain_regular_dsqid(s32 cpu) {
    cpu = clip_cpu_to_bounds(cpu);
    return cpu_to_domain_id[cpu] + DSQ_REG_Q_PER_DOM_START;
}

struct global_reserved_corebitmask_kfunc_map_value {
    struct bpf_cpumask __kptr *bpf_cpumask;
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, int);
    __type(value, struct global_reserved_corebitmask_kfunc_map_value);
    __uint(max_entries, 1);
} global_reserved_corebitmask_kfunc_map SEC(".maps");

__always_inline struct global_reserved_corebitmask_kfunc_map_value *
get_global_reserved_corebitmask_map_value() {
    s32 err;
    int key = 0;
    struct global_reserved_corebitmask_kfunc_map_value *v;

    v = bpf_map_lookup_elem(&global_reserved_corebitmask_kfunc_map, &key);
    if (!v) {
        struct global_reserved_corebitmask_kfunc_map_value default_v;
        default_v.bpf_cpumask = NULL;
        err = bpf_map_update_elem(&global_reserved_corebitmask_kfunc_map, &key,
                                  (const void *)&default_v, BPF_ANY);
        if (err < 0) {
            return NULL;
        }

        v = bpf_map_lookup_elem(&global_reserved_corebitmask_kfunc_map, &key);
        if (!v) {
            return NULL;
        }

        err = calloc_cpumask(&v->bpf_cpumask);
        if (err < 0) {
            return NULL;
        }
    }
    return v;
}

static __noinline void global_reserved_corebitmask_or_and_store(const struct cpumask *corebitmask) {
    s32 err;
    struct global_reserved_corebitmask_kfunc_map_value *v;

    v = get_global_reserved_corebitmask_map_value();
    if (!v) {
        return;
    }

    if (v->bpf_cpumask) {
        struct bpf_cpumask *map_mask = NULL;

        map_mask = bpf_kptr_xchg(&v->bpf_cpumask, NULL);
        if (map_mask) {
            bpf_cpumask_or(map_mask, (const struct cpumask *)map_mask, corebitmask);
            map_mask = bpf_kptr_xchg(&v->bpf_cpumask, map_mask);
            if (map_mask) {
                bpf_cpumask_release(map_mask);
            }
        }
    }
}

static __noinline bool global_reserved_corebitmask_is_set(s32 cpu) {
    s32 err;
    struct global_reserved_corebitmask_kfunc_map_value *v;

    v = get_global_reserved_corebitmask_map_value();
    if (!v) {
        return false;
    }

    if (v->bpf_cpumask) {
        return bpf_cpumask_test_cpu(cpu, v->bpf_cpumask);
    }

    return false;
}

static long __noinline callback_gmap_populate_reserved_corebitmask_iter(struct bpf_map *map,
                                                                        const void *key, void *val,
                                                                        void *ctx) {

    if (!key || !val) {
        return 1;
    }

    SchedGroupID *gid = (SchedGroupID *)key;
    SchedGroupChrs_t *chrs = (SchedGroupChrs_t *)val;

    global_reserved_corebitmask_or_and_store(&chrs->reserved_corebitmask);
    return 0;
}

static s32 __noinline create_global_reserved_corebitmask() {

    int count;
    count =
        bpf_for_each_map_elem(&gMap, &callback_gmap_populate_reserved_corebitmask_iter, &count, 0);
    info("[dsqs][gmap] iterated total of %d elements to populate reserved corebitmask", count);

    return 0;
}

static s32 __noinline create_priority_dsqs_per_cpu() {
    s32 cpu;
    s32 numa_node;
    u64 dsqid;
    s32 err = 0;

    bpf_for(cpu, 0, nr_cpu_ids_config) {
        numa_node = cpu_to_numanode(cpu);
        dsqid = DSQ_PRIO_PER_CPU_START + cpu;
        err = scx_bpf_create_dsq(dsqid, numa_node);
        if (err < 0) {
            return err;
        }
    }

    return err;
}

// bpf verifier does not allow scx_bpf_dsq_move_to_local call within a
// bpf_for loop. This function only moves three tasks at max.
static s32 __noinline move_from_custom_queue_to_local_dsq(u64 dsqid, s32 task_count) {
    s32 tasks_moved = 0;

    if (task_count != tasks_moved && scx_bpf_dsq_move_to_local(dsqid, 0)) {
        dsq_stats_task_consumed(dsqid);
        tasks_moved += 1;
    }
    if (task_count != tasks_moved && scx_bpf_dsq_move_to_local(dsqid, 0)) {
        tasks_moved += 1;
    }
    if (task_count != tasks_moved && scx_bpf_dsq_move_to_local(dsqid, 0)) {
        tasks_moved += 1;
    }

    return tasks_moved;
}

static __noinline s32 move_from_custom_queue_to_local_dsq_if_over_period(u64 dsqid,
                                                                         s32 task_count) {
    struct dsq_context *dsq_context = try_lookup_dsq_context(dsqid);
    if (!dsq_context) {
        return 0;
    }

    u64 last_consume_time = dsq_context->last_consume_time;
    u64 current_time = bpf_ktime_get_tai_ns();

    // corner case for the starting point
    if (last_consume_time == 0) {
        dsq_context->last_consume_time = current_time;
        last_consume_time = current_time;
    }

    u64 time_since = current_time - last_consume_time;

    if (time_since > REGULAR_QUEUE_CONSUME_PERIOD_THRESHOLD) {
        return move_from_custom_queue_to_local_dsq(dsqid, task_count);
    }
    return 0;
}

static __noinline s32 move_from_per_cpu_custom_to_local_dsq(s32 cpu, s32 task_count) {
    return move_from_custom_queue_to_local_dsq(DSQ_PRIO_PER_CPU_START + cpu, task_count);
}

static __inline s32 local_and_custom_dsq_len_for(s32 cpu) {
    return scx_bpf_dsq_nr_queued(SCX_DSQ_LOCAL_ON | cpu) +
           scx_bpf_dsq_nr_queued(DSQ_PRIO_PER_CPU_START + cpu);
}

static __noinline u64 tasks_enqueued_on_bpfscheduler() {
    u64 dsqid;
    u64 max_dsqs = domains_count;
    max_dsqs = max_dsqs > DSQ_MAX_COUNT ? DSQ_MAX_COUNT : max_dsqs;
    u64 count = 0;

    bpf_for(dsqid, DSQ_PRIO_Q_PER_DOM_START, DSQ_PRIO_Q_PER_DOM_START + max_dsqs) {
        count += scx_bpf_dsq_nr_queued(dsqid);
    }

    bpf_for(dsqid, DSQ_REG_Q_PER_DOM_START, DSQ_REG_Q_PER_DOM_START + max_dsqs) {
        count += scx_bpf_dsq_nr_queued(dsqid);
    }

    count += scx_bpf_dsq_nr_queued(DSQ_GLOBAL_Q_ID);

    return count;
}

static __noinline void check_dsqlen_of_each_dsq_for_tracing() { tasks_enqueued_on_bpfscheduler(); }

static bool __inline lookup_and_update_min_dsq_len(s32 current_cpu, s32 *old_cpu, s32 *old_len) {
    if (!old_cpu || !old_len) {
        return false;
    }

    s32 current_len = local_and_custom_dsq_len_for(current_cpu);
    if (current_len < *old_len) {
        *old_cpu = current_cpu;
        *old_len = current_len;
        return true;
    }

    return false;
}

static __noinline s32 least_loaded_local_dsq_cpu(struct cpumask *bitmask) {
    s32 cpu;
    s32 min_dsq_cpu = 0;
    s32 min_dsq_len = INT_MAX;

    bpf_for(cpu, 0, MAX_CPUS) {
        if (bitmask) {
            if (bpf_cpumask_test_cpu(cpu, bitmask)) {
                lookup_and_update_min_dsq_len(cpu, &min_dsq_cpu, &min_dsq_len);
            }
        } else {
            lookup_and_update_min_dsq_len(cpu, &min_dsq_cpu, &min_dsq_len);
        }
    }

    return min_dsq_cpu;
}

static __noinline s32 worksteal_from_n_neighbors(s32 cpu, s32 n, s32 task_count) {
    // n: N, > 0
    // stealing from n neighbors on each side of the cpu
    cpu = clip_cpu_to_bounds(cpu);

    s32 start_cpu = cpu - n;
    s32 end_cpu = cpu + n;

    start_cpu = clip_cpu_to_bounds(start_cpu);
    end_cpu = clip_cpu_to_bounds(end_cpu);

    s32 tasks_leftover = task_count;
    s32 tasks_moved = 0;

    s32 cpu_i;
    bpf_for(cpu_i, cpu, end_cpu + 1) {
        tasks_moved += move_from_per_cpu_custom_to_local_dsq(cpu_i, tasks_leftover);
        tasks_leftover = task_count - tasks_moved;
        if (tasks_leftover == 0) {
            return tasks_moved;
        }
    }
    bpf_for(cpu_i, start_cpu, cpu + 1) {
        tasks_moved += move_from_per_cpu_custom_to_local_dsq(cpu_i, tasks_leftover);
        tasks_leftover = task_count - tasks_moved;
        if (tasks_leftover == 0) {
            return tasks_moved;
        }
    }

    return tasks_moved;
}

static __noinline void worksteal_from_neighbors(s32 cpu, s32 task_count) {
    s32 neighbor_count = MAX(cpu, MAX_CPUS - cpu);
    s32 n;
    s32 tasks_leftover = task_count;
    bpf_for(n, 1, neighbor_count) {
        tasks_leftover = tasks_leftover - worksteal_from_n_neighbors(cpu, n, tasks_leftover);
        if (tasks_leftover == 0) {
            return;
        }
    }
}

static __noinline s32 worksteal_from_neighbors_domain_queue(bool from_highpriority_queue, s32 cpu,
                                                            s32 task_count) {
    u64 dsqid = from_highpriority_queue ? cpu_to_domain_highpriority_dsqid(cpu)
                                        : cpu_to_domain_regular_dsqid(cpu);
    u64 starting_offset =
        from_highpriority_queue ? DSQ_PRIO_Q_PER_DOM_START : DSQ_REG_Q_PER_DOM_START;
    u64 domain_id = dsqid - starting_offset;

    s32 neighbor_count = MAX(domain_id, domains_count - domain_id);
    neighbor_count = clip_dsqid_to_bounds(neighbor_count, 0);

    s32 tasks_leftover = task_count;
    s32 tasks_moved = 0;

    u64 steal_from_dsqid;

    // bpf_for causes verifier to panic saying program is too complex
#define STEAL_FROM_NTH_NEIGHBOR(nth)                                                               \
    steal_from_dsqid = dsqid - nth;                                                                \
    steal_from_dsqid = clip_dsqid_to_bounds(steal_from_dsqid, starting_offset);                    \
    tasks_moved += move_from_custom_queue_to_local_dsq(steal_from_dsqid, tasks_leftover);          \
    tasks_leftover = task_count - tasks_moved;                                                     \
    if (tasks_leftover == 0) {                                                                     \
        return tasks_moved;                                                                        \
    }
    STEAL_FROM_NTH_NEIGHBOR(1)
    STEAL_FROM_NTH_NEIGHBOR(-1)
    STEAL_FROM_NTH_NEIGHBOR(2)
    STEAL_FROM_NTH_NEIGHBOR(-2)
    STEAL_FROM_NTH_NEIGHBOR(3)
    STEAL_FROM_NTH_NEIGHBOR(-3)
    STEAL_FROM_NTH_NEIGHBOR(4)
    STEAL_FROM_NTH_NEIGHBOR(-4)
    STEAL_FROM_NTH_NEIGHBOR(5)
    STEAL_FROM_NTH_NEIGHBOR(-5)
    STEAL_FROM_NTH_NEIGHBOR(6)
    STEAL_FROM_NTH_NEIGHBOR(-6)
    STEAL_FROM_NTH_NEIGHBOR(7)
    STEAL_FROM_NTH_NEIGHBOR(-7)
    STEAL_FROM_NTH_NEIGHBOR(8)
    STEAL_FROM_NTH_NEIGHBOR(-8)
    STEAL_FROM_NTH_NEIGHBOR(9)
    STEAL_FROM_NTH_NEIGHBOR(-9)
    STEAL_FROM_NTH_NEIGHBOR(10)
    STEAL_FROM_NTH_NEIGHBOR(-10)
    STEAL_FROM_NTH_NEIGHBOR(11)
    STEAL_FROM_NTH_NEIGHBOR(-11)

    return tasks_moved;
}

static __noinline u64 shorten_timeslice_by_factor(u64 timeslice, u64 factor) {
    factor = factor > 0 ? factor : 1;

    timeslice = timeslice / factor;
    if (timeslice < MIN_TS) {
        timeslice = MIN_TS;
    }

    return timeslice;
}

static __noinline u64 shorten_timeslice_by_cputime_to_timeslice_ratio(u64 timeslice,
                                                                      struct task_struct *p) {
    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return timeslice;
    }

    u64 cpu_time = tctx->cpu_time_avg;
    u64 cputime_by_timeslice = (cpu_time * 100) / timeslice; // %[1,100]

    info("[timeslice_shortening][%s:%d] cputime_by_timeslice %llu ", p->comm, p->pid,
         cputime_by_timeslice);

    // to penalize tasks that consume whole timeslice
    cputime_by_timeslice = cputime_by_timeslice / 10;
    return shorten_timeslice_by_factor(timeslice, cputime_by_timeslice);
}

static __noinline u64 shorten_timeslice_by_dsqlen(u64 timeslice, u64 dsqid) {
    s32 current_len = scx_bpf_dsq_nr_queued(dsqid);
    return shorten_timeslice_by_factor(timeslice, current_len);
}

static __noinline bool enqueue_to_assigned_domain_queue(struct task_struct *p,
                                                        bool to_highpriority_queue, bool fifo) {
    if (!p) {
        return false;
    }

    CgroupChrs_t *cgrp_chrs = get_cgroup_chrs_for_p(p);
    if (!cgrp_chrs) {
        return false;
    }

    SchedGroupChrs_t *sched_chrs = get_schedgroup_chrs(cgrp_chrs->gid);
    if (!sched_chrs) {
        return false;
    }

    u64 timeslice = sched_chrs->timeslice * NSEC_PER_MSEC;
    u64 dsqid = to_highpriority_queue ? DSQ_PRIO_Q_PER_DOM_START : DSQ_REG_Q_PER_DOM_START;
    dsqid = dsqid + cgrp_chrs->gid;

    if (fifo) {
        scx_bpf_dsq_insert(p, dsqid, timeslice, 0);
    } else {
        struct task_context *tctx = try_lookup_task_ctx(p);
        if (!tctx) {
            return false;
        }

        last_max_vtime = MAX(tctx->vtime, last_max_vtime);
        timeslice = timeslice / tctx->busypolling_factor;

        scx_bpf_dsq_insert_vtime(p, dsqid, timeslice, tctx->vtime, 0);
        info("[task_stats][%s:%d] inserted to dsq(0x%x) vtime: %llu timeslice: %llu ", p->comm, p->pid, dsqid,
             tctx->vtime, timeslice);
    }

    info("[task_stats][%s:%d] inserted to dsq(0x%x) ", p->comm, p->pid, dsqid);
    if (scx_bpf_dsq_nr_queued(dsqid) > bpf_cpumask_weight(&sched_chrs->corebitmask)) {
      kick_all_cpus();
    } else {
      kick_cpus(&sched_chrs->corebitmask);
    }
    return true;
}

static __noinline bool enqueue_to_last_domain_queue(struct task_struct *p,
                                                    bool to_highpriority_queue) {
    if (!p) {
        return false;
    }

    struct task_context *tctx = try_lookup_task_ctx(p);
    if (!tctx) {
        return false;
    }

    u64 dsqid = cpu_to_domain_regular_dsqid(MAX_CPUS - 1);
    u64 domain_id = dsqid - DSQ_REG_Q_PER_DOM_START;

    SchedGroupChrs_t *sched_chrs = get_schedgroup_chrs(domain_id);
    if (!sched_chrs) {
        return false;
    }

    u64 timeslice = sched_chrs->timeslice * NSEC_PER_MSEC;
    dsqid = to_highpriority_queue ? DSQ_PRIO_Q_PER_DOM_START : DSQ_REG_Q_PER_DOM_START;
    dsqid = dsqid + domain_id;

    info("[task_stats][%s:%d] insert to dsq(0x%x) tctx->vtime %llu ", p->comm, p->pid, dsqid,
         tctx->vtime);
    last_max_vtime = MAX(tctx->vtime, last_max_vtime);
    scx_bpf_dsq_insert_vtime(p, dsqid, timeslice, tctx->vtime, 0);
    kick_cpus(&sched_chrs->corebitmask);
    return true;
}

static __noinline void enqueue_to_global_queue(struct task_struct *p, bool fifo) {
    if (!p) {
        return;
    }

    u64 timeslice = DEFAULT_TS;
    u64 dsqid = DSQ_GLOBAL_Q_ID;

    if (fifo) {
        scx_bpf_dsq_insert(p, dsqid, timeslice, 0);
    } else {
        struct task_context *tctx = try_lookup_task_ctx(p);
        if (!tctx) {
            return;
        }

        last_max_vtime = MAX(tctx->vtime, last_max_vtime);
        scx_bpf_dsq_insert_vtime(p, dsqid, timeslice, tctx->vtime, 0);
        info("[task_stats][%s:%d] inserted to dsq(0x%x) vtime: %llu ", p->comm, p->pid, dsqid,
             tctx->vtime);
    }

    info("[task_stats][%s:%d] inserted to dsq(0x%x) ", p->comm, p->pid, dsqid);
    kick_all_cpus();
}

static __noinline void enqueue_to_per_cpu_custom_dsq(struct task_struct *p) {
    s32 cpu = bpf_get_smp_processor_id();
    u64 timeslice = DEFAULT_TS;
    u64 dsqid;

    cpu = least_loaded_local_dsq_cpu(p->cpus_ptr);
    dsqid = DSQ_PRIO_PER_CPU_START + cpu;
    scx_bpf_dsq_insert(p, dsqid, timeslice, 0);
    kick_cpus(p->cpus_ptr);

    info("[task_stats][%s:%d] inserted to dsq(0x%x) ", p->comm, p->pid, dsqid);
}

////////////////////////////
// Scheduling Logic Helpers

//////
// Task -> SCX switch

static void __noinline switch_to_scx_if_cgroup_exists(struct task_struct *p) {

    if (!bpf_ksym_exists(scx_bpf_switch_to_scx)) {
        return;
    }

    char *name = get_schedcgroup_name(p);
    if (!name) {
        return;
    }

    cgroup_ctx_t *cgroup_ctx = bpf_map_lookup_elem(&cgroup_ctx_stor, name);
    if (!cgroup_ctx) {
        return;
    }

    scx_bpf_switch_to_scx(p);
    info("[task_stats][%s:%d] switched to scx ", p->comm, p->pid);
}

//////
// Task -> Sched CPU, TS

static u64 __always_inline fifo_vtime() { return bpf_ktime_get_ns(); }

static u64 __noinline wakeup_boost_to_front_of_queue(u64 vtime, s32 cpu, u64 timeslice) {
    s32 current_len = local_and_custom_dsq_len_for(cpu);
    return vtime - (timeslice * current_len);
}

static void __noinline update_from_assigned_domain(s32 *cpu, u64 *timeslice,
                                                   struct task_struct *p) {
    if (!cpu || !timeslice || !p) {
        return;
    }

    CgroupChrs_t *cgrp_chrs = get_cgroup_chrs_for_p(p);
    if (cgrp_chrs != NULL) {
        SchedGroupChrs_t *sched_chrs = get_schedgroup_chrs(cgrp_chrs->gid);
        if (sched_chrs != NULL) {
            struct bpf_cpumask *allowed_cpus_mask =
                cpu_mask_intersection(p->cpus_ptr, &sched_chrs->corebitmask);
            if (allowed_cpus_mask) {
                if (!bpf_cpumask_empty((struct cpumask *)allowed_cpus_mask)) {
                    *cpu = least_loaded_local_dsq_cpu((struct cpumask *)allowed_cpus_mask);
                }
                bpf_cpumask_release(allowed_cpus_mask);
            }

            *timeslice = sched_chrs->timeslice * NSEC_PER_MSEC;
        }
    }
}

static void __noinline update_from_assigned_domain_for_high_priority_tasks(s32 *cpu, u64 *timeslice,
                                                                           struct task_struct *p) {
    if (!cpu || !timeslice || !p) {
        return;
    }

    CgroupChrs_t *cgrp_chrs = get_cgroup_chrs_for_p(p);
    if (cgrp_chrs != NULL) {
        SchedGroupChrs_t *sched_chrs = get_schedgroup_chrs(cgrp_chrs->gid);
        if (sched_chrs != NULL) {
            struct bpf_cpumask *allowed_cpus_mask =
                cpu_mask_intersection(p->cpus_ptr, &sched_chrs->reserved_corebitmask);
            if (allowed_cpus_mask) {
                if (!bpf_cpumask_empty((struct cpumask *)allowed_cpus_mask)) {
                    *cpu = least_loaded_local_dsq_cpu((struct cpumask *)allowed_cpus_mask);
                }
                bpf_cpumask_release(allowed_cpus_mask);
            }

            *timeslice = sched_chrs->timeslice * NSEC_PER_MSEC;
        }
    }
}

static s32 __noinline get_sched_cpu_ts(struct task_struct *p, s32 *cpu, u64 *ts) {

    if (p == NULL || cpu == NULL || ts == NULL) {
        goto out_no_alloc;
    }

    SchedGroupChrs_t *sched_chrs = get_schedchrs_cached(p);
    // SchedGroupChrs_t *sched_chrs = get_schedchrs( p );
    if (sched_chrs == NULL || sched_chrs->id == -1) {
        goto out_no_alloc;
    }

    *cpu = scx_bpf_pick_any_cpu(&sched_chrs->corebitmask, SCX_PICK_IDLE_CORE);
    *ts = sched_chrs->timeslice;

    info("[get_sched_cpu_ts] gid: %d picked cpu %d with timeslice %d ms task %d - %s",
         sched_chrs->id, *cpu, *ts, p->pid, p->comm);

    if (!bpf_cpumask_test_cpu(*cpu, p->cpus_ptr)) {
        info("[warn][get_sched_cpu_ts] cpu was not allowed %d task %d - %s", cpu, p->pid, p->comm);
        *cpu = scx_bpf_pick_any_cpu(p->cpus_ptr, SCX_PICK_IDLE_CORE);
        info("[warn][get_sched_cpu_ts] changed to %d task %d - %s", cpu, p->pid, p->comm);
    }

    return 0;

out_no_alloc:
    return -1;
}

#define PRIO_TASKCOUNT_FACTOR (100 * NSEC_PER_MSEC)
static u64 __always_inline prio_taskcount_tconsumed(u64 cvtime, u64 tconsumed,
                                                    u64 cgroup_tskcnt_prio) {
    return cvtime + ((1 - cgroup_tskcnt_prio) * PRIO_TASKCOUNT_FACTOR + tconsumed);
}

static u64 __always_inline prio_plain_tconsumed(u64 cvtime, u64 tconsumed) {
    u64 vtime = cvtime + tconsumed;
    return vtime;
}

static u64 __always_inline prio_invoke_time(u64 cvtime, u64 invoke, u64 tconsumed) {
    invoke = invoke == 0 ? 10 : invoke;
    u64 vtime = cvtime + (tconsumed * invoke) / NSEC_PER_MSEC;
    return vtime;
}

static u64 __always_inline prio_short_duration_unweighted(u64 cvtime, u64 dur, u64 tconsumed) {
    // tconsumed is in ns and workerdur is also in ms
    // 1000000000*1000000 - therefore we dividd by 1000000000 - to get back into ms
    dur = dur == 0 ? 10 : dur;
    u64 vtime = cvtime + dur * NSEC_PER_USEC;
    return vtime;
}

static u64 __always_inline prio_short_duration(u64 cvtime, u64 dur, u64 tconsumed) {
    // tconsumed is in ns and workerdur is also in ms
    // 1000000000*1000000 - therefore we dividd by 1000000000 - to get back into ms
    dur = dur == 0 ? 10 : dur;
    u64 vtime = cvtime + (tconsumed * dur) / NSEC_PER_MSEC;
    return vtime;
}

u64 win_sched_bound = 0;
static u64 __always_inline prio_short_first_reset(u64 cvtime, u64 dur, u64 tconsumed) {
    if (win_sched_bound < dur) {
        win_sched_bound = dur;
    }

    // it's not really srpt - since every new task starts from the window
    // they are all equally prioritized
    if (cvtime < tconsumed) {
        cvtime = (win_sched_bound * NSEC_PER_MSEC);
    } else {
        cvtime -= tconsumed;
    }

    return cvtime;
}

static u64 __always_inline prio_short_first_over(u64 cvtime, u64 tconsumed) {
    // we deliberately leave the vtime close to zero
    // when a new task would be created it's given twice
    // the latency associated with it's group
    if (cvtime > tconsumed) {
        cvtime -= tconsumed;
    } else {
        cvtime = 0;
    }

    return cvtime;
}

#endif // __UTILS_F
