# Scratch Pad for constrained policy 


```
    info_msg( "%s - cpu_dsq(%d,%d) cpu_dsq(%d,%d) cpu_dsq(%d,%d) cpu_dsq(%d,%d) ", 
             __func__, 
             2, scx_bpf_dsq_nr_queued( SCX_DSQ_LOCAL_ON | 2 ),
             3, scx_bpf_dsq_nr_queued( SCX_DSQ_LOCAL_ON | 3 ),
             4, scx_bpf_dsq_nr_queued( SCX_DSQ_LOCAL_ON | 4 ),
             5, scx_bpf_dsq_nr_queued( SHARED_DSQ )
           );
```
