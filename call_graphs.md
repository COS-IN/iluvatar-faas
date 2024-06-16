
# PID generation 

  run_container 
    send_bg_packet
      wake up bg_workqueue
        pushes the pid, fqdn into the map


# Enqueuing a function 

  queueing_dipatcher 
    load_balance/mod.rs
    iluvatar_worker.rs|121
      invoke_async
        async_invocation
      invoke
        sync_invocation
            enqueue_new_invocation

# Removing a function container 

  remove_container
    remove_container_internal
      kill_task
      delete_task
      delete_containerd_container
      delete_container_resources







