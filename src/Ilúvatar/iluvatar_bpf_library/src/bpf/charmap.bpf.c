#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define MAX_FUNCS 50 
#define FUNC_METADATA_KEYSIZE 15 // because the kernel fs inode name is 15 characters 

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
  __uint(key_size, sizeof(char)*FUNC_METADATA_KEYSIZE);         /* cgrp ID */
  __uint(value_size, sizeof(CharVal_t)); /* Value Structure */
} func_metadata SEC(".maps");

char _license[] SEC("license") = "GPL";

