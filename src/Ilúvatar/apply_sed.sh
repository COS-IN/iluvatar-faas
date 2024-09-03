#!/bin/bash

get_real_path() {
    script_path=$(dirname $1)
    script_path=$(realpath $script_path)
}
if [[ $0 != $BASH_SOURCE ]]; then
    get_real_path $BASH_SOURCE
else
    get_real_path $0
fi
cd $script_path


fs=(
    ./fs_policy_locality/src/main.rs
    ./fs_policy_lavd/src/main.rs
    ./fs_policy_constrained/src/main.rs
    iluvatar_worker_library/src/worker_api/mod.rs
    iluvatar_worker_library/src/services/invocation/queueing_dispatcher.rs
    iluvatar_worker_library/src/services/invocation/queueing_dispatcher.rs
    iluvatar_worker_library/src/services/invocation/energy_limiter.rs
    iluvatar_worker_library/src/services/invocation/gpu_q_invoke.rs
    iluvatar_worker_library/src/services/invocation/cpu_q_invoke.rs
    iluvatar_worker_library/src/services/invocation/mod.rs
    iluvatar_worker_library/src/services/invocation/mod.rs
    iluvatar_worker_library/src/services/invocation/queueing/avail_scale.rs
    iluvatar_worker_library/src/services/invocation/queueing/minheap_ed.rs
    iluvatar_worker_library/src/services/invocation/queueing/fcfs_gpu.rs
    iluvatar_worker_library/src/services/invocation/queueing/oldest_gpu.rs
    iluvatar_worker_library/src/services/invocation/queueing/oldest_gpu.rs
    iluvatar_worker_library/src/services/invocation/queueing/fcfs.rs
    iluvatar_worker_library/src/services/invocation/queueing/minheap_iat.rs
    iluvatar_worker_library/src/services/invocation/queueing/gpu_mqfq.rs
    iluvatar_worker_library/src/services/invocation/queueing/cold_priority.rs
    iluvatar_worker_library/src/services/invocation/queueing/minheap.rs
    iluvatar_worker_library/src/services/invocation/queueing/dynamic_batching.rs
    iluvatar_worker_library/src/services/invocation/queueing/wfq.rs
    iluvatar_worker_library/src/services/invocation/queueing/concur_mqfq.rs
    iluvatar_worker_library/src/services/invocation/queueing/mod.rs
    iluvatar_worker_library/tests/container_sim_tests.rs
    ./fs_policy_powof2/src/main.rs
    ./fs_policy_fifo/src/main.rs
)


for f in ${fs[@]}; do 
    sed -i \
        's/use.*characteristics_map/use iluvatar_worker_library::utils::characteristics_map/g' \
        $f 
done



