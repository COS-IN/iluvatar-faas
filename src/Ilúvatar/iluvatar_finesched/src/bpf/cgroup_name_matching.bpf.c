// reference: adapted from layered scheduler of scx project

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(u32));
    /* double size because verifier can't follow length calculation */
    __uint(value_size, 2 * MAX_PATH);
    __uint(max_entries, 1);
} cgrp_path_bufs SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(u32));
    __uint(value_size, MAX_PATH);
    __uint(max_entries, 1);
} prefix_bufs SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(u32));
    __uint(value_size, MAX_PATH);
    __uint(max_entries, 1);
} str_bufs SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(key_size, sizeof(u32));
    __uint(value_size, MAX_PATH);
    __uint(max_entries, 1);
} ln_bufs SEC(".maps");

// create a cgroup path for the given cgroup
static char *format_cgrp_path(struct cgroup *cgrp) {
    u32 zero = 0;
    char *path = bpf_map_lookup_elem(&cgrp_path_bufs, &zero);
    if (path) {
        memset(path, 0, 2 * MAX_PATH);
    }
    u32 len = 0, level, max_level;

    if (!path) {
        scx_bpf_error("cgrp_path_buf lookup failed");
        return NULL;
    }

    max_level = cgrp->level;
    if (max_level > 127)
        max_level = 127;

    bpf_for(level, 1, max_level + 1) {
        int ret;

        if (level > 1 && len < MAX_PATH - 1)
            path[len++] = '/';

        if (len >= MAX_PATH - 1) {
            /* Long, unrelated cgroup paths are expected on the host. A path
             * we cannot represent is simply not a FineSched match; it must
             * not disable the scheduler for every task. */
            dbg("cgrp_path_buf overflow; skipping cgroup at level %u", level);
            return NULL;
        }

        ret = bpf_probe_read_kernel_str(path + len, MAX_PATH - len - 1,
                                        BPF_CORE_READ(cgrp, ancestors[level], kn, name));
        if (ret < 0) {
            scx_bpf_error("bpf_probe_read_kernel_str failed");
            return NULL;
        }

        len += ret - 1;
    }

    if (len >= MAX_PATH - 2) {
        dbg("cgrp_path_buf overflow; skipping cgroup path");
        return NULL;
    }
    path[len] = '/';
    path[len + 1] = '\0';

    return path;
}

static __always_inline char *get_schedcgroup_path(struct task_struct *p) {
    struct cgroup *cgrp;
    char *path = NULL;

    bpf_rcu_read_lock();
    // cgroups->dfl_cgrp is the cgroup-id 1
    // we need cgroups->subsys[sched] cgroup
    if (p->sched_task_group && p->sched_task_group->css.cgroup) {
        path = format_cgrp_path(p->sched_task_group->css.cgroup);
    }
    bpf_rcu_read_unlock();

    return path;
}

static char *__noinline get_last_node(char *path, u32 max_len) {
    // path is of the form a/b/ - there is always a trailing /
    if (!path || max_len > MAX_PATH) {
        return NULL;
    }

    u32 zero = 0;
    u32 llast = 0;
    u32 last = 0;
    u32 i;
    u32 id_s;
    char *next = NULL;
    char *ln_buf = bpf_map_lookup_elem(&ln_bufs, &zero);
    if (ln_buf) {
        memset(ln_buf, 0, max_len);
    }

    bpf_for(i, 0, max_len) {
        next = &path[i];
        if (next) {
            if (*next == '/') {
                llast = last;
                last = i + 1;
            }
        }
    }

    if (last > 1) {
        path[last - 1] = '\0';
    }

    bpf_for(i, 0, max_len) {
        next = &ln_buf[i];
        if (next) {
            *next = path[i + llast];
            id_s += 1;
        }
    }

    return ln_buf;
}

static char *__noinline get_schedcgroup_name(struct task_struct *p) {

    char *cgrp_path;
    cgrp_path = get_schedcgroup_path(p);
    if (cgrp_path) {
        dbg("[cgroup][schedcgroup] got cgroup path task %d - %s cgroup %s ", p->pid, p->comm,
            cgrp_path);
    }

    char *stripped = get_last_node(cgrp_path, MAX_PATH);
    if (stripped) {
        dbg("[cgroup][schedcgroup] stripped last node cgroup name task %d - %s cgroup %s ", p->pid,
            p->comm, stripped);
    }

    return stripped;
}

bool __noinline match_prefix(const char *prefix, const char *str, u32 max_len) {
    u32 c, zero = 0;
    int len;

    if (!prefix || !str || max_len > MAX_PATH) {
        scx_bpf_error("invalid args: %s %s %u", prefix, str, max_len);
        return false;
    }

    char *pre_buf = bpf_map_lookup_elem(&prefix_bufs, &zero);
    char *str_buf = bpf_map_lookup_elem(&str_bufs, &zero);
    if (!pre_buf || !str_buf) {
        scx_bpf_error("failed to look up buf");
        return false;
    }

    len = bpf_probe_read_kernel_str(pre_buf, MAX_PATH, prefix);
    if (len < 0) {
        scx_bpf_error("failed to read prefix");
        return false;
    }

    len = bpf_probe_read_kernel_str(str_buf, MAX_PATH, str);
    if (len < 0) {
        scx_bpf_error("failed to read str");
        return false;
    }

    bpf_for(c, 0, max_len) {
        c &= 0xfff;
        if (c > len) {
            scx_bpf_error("invalid length");
            return false; /* appease the verifier */
        }
        if (pre_buf[c] == '\0')
            return true;
        if (str_buf[c] != pre_buf[c])
            return false;
    }
    return false;
}
