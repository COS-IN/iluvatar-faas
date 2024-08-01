
# Common Patterns for writing BPF programs 

## Data Structures 

### Arrays 

  * [defined and documented in schedext tools in kernel source](kernel/tools/sched_ext/include/scx/common.bpf.h)

  Creating resizable array 
```
  const volatile u32 RESIZABLE_ARRAY(rodata, in_pair_idx);

	vptr = (u32 *)ARRAY_ELEM_PTR(pair_id, cpu, nr_cpu_ids);

```

  Index and add a value to an element of array. 
```
  static u64 cgrp_q_len[MAX_CGRPS];

	cgq_len = MEMBER_VPTR(cgrp_q_len, [*q_idx]);
	if (!cgq_len) {
		scx_bpf_error("MEMBER_VTPR malfunction");
		return;
	}

	if (!__sync_fetch_and_add(cgq_len, 1) &&
	    bpf_map_push_elem(&top_q, &cgid, 0)) {
		scx_bpf_error("top_q overflow");
		return;
	}
```

### Maps 

  key-value pair 
```
  // a hash map 
  struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, MAX_CGRPS);
    __uint(key_size, sizeof(u64));		/* cgrp ID */
    __uint(value_size, sizeof(s32));	/* cgrp_q idx */
  } cgrp_q_idx_hash SEC(".maps");

  // an array 
  struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, u32);
    __type(value, struct pair_ctx);
  } pair_ctx SEC(".maps");
```

  value queue 
```
  struct cgrp_q {
    __uint(type, BPF_MAP_TYPE_QUEUE);
    __uint(max_entries, MAX_QUEUED);
    __type(value, u32);
  };
```

  Array of maps, in a way that a[1] would be map  
```
  struct {
    __uint(type, BPF_MAP_TYPE_ARRAY_OF_MAPS);
    __uint(max_entries, MAX_CGRPS);
    __type(key, s32);
    __array(values, struct cgrp_q);
  } cgrp_q_arr SEC(".maps");
```


## Locks 

```
	struct bpf_spin_lock	lock;
```

## APIs 


	__sync_fetch_and_add(&nr_total, 1);


