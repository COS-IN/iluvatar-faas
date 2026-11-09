use crate::bpf_fsched::*;
use crate::pin_map;
use crate::reuse_pinned_map;
use crate::rm_pinned_map;
use crate::topology::SystemTopology;
use crate::CORE_METRICS_MAP_PATH;
use crate::CGROUP_MAP_PATH;
use crate::SCHED_GROUP_MAP_PATH;
use crate::SCHED_GROUP_STATS_MAP_PATH;

use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::Link;
use libbpf_rs::OpenObject;

use scx_utils::perf;
use scx_utils::scx_ops_attach;
use scx_utils::scx_ops_load;
use scx_utils::scx_ops_open;
use scx_utils::try_set_rlimit_infinity;
use scx_utils::uei_exited;
use scx_utils::uei_report;

fn attach_perf_hw_cycles_event<'a>(skel: &mut BpfSkel<'a>, topology: &SystemTopology) -> Result<Vec<Link>> {
    let mut perf_links = vec![];

    for cpu_id in topology.online_cpu_ids() {
        // Create custom perf event attributes for sampling with IP collection
        let mut attr: perf::bindings::perf_event_attr = unsafe { std::mem::zeroed() };
        attr.size = std::mem::size_of::<perf::bindings::perf_event_attr>() as u32;
        attr.type_ = perf::bindings::PERF_TYPE_HARDWARE;
        attr.config = perf::bindings::PERF_COUNT_HW_CPU_CYCLES as u64;

        // Configure for sampling with instruction pointer collection
        attr.sample_type = perf::bindings::PERF_SAMPLE_IP as u64;
        attr.__bindgen_anon_1.sample_period = 1000000 as u64; // Mhz
        attr.set_freq(0);
        attr.set_disabled(0);
        attr.set_exclude_kernel(0);
        attr.set_exclude_hv(0);
        attr.set_inherit(0);
        attr.set_pinned(1);

        // Use scx_utils perf event helper to open the perf event
        let perf_fd = unsafe {
            perf::perf_event_open(
                &mut attr as *mut perf::bindings::perf_event_attr,
                -1,            // pid (-1 for all processes)
                cpu_id as i32, // cpu
                -1,            // group_fd
                0,             // flags
            )
        };

        if perf_fd <= 0 {
            let err = std::io::Error::last_os_error();
            bail!("Failed to open perf event for CPU {cpu_id}: {err}");
        }

        // Attach BPF program to the perf event
        match skel.progs.perf_sample_handler.attach_perf_event(perf_fd) {
            Ok(link) => {
                // Enable the perf event using scx_utils ioctl helper
                if unsafe { perf::ioctls::enable(perf_fd, 0) } < 0 {
                    let err = std::io::Error::last_os_error();
                    bail!("Failed to enable perf event for CPU {cpu_id}: {err}");
                }
                perf_links.push(link);
            },
            Err(_e) => unsafe {
                libc::close(perf_fd);
                return Err(_e).context("Failed to attach perf event for CPU {cpu_id}: {err}");
            },
        }
    }

    println!("HW CPU Cycles event attached to all cpus");
    Ok(perf_links)
}

fn load_bpf_scheduler<'a>(
    verbose: u8,
    use_switch_kfunc: bool,
    task_ip_monitoring: bool,
    topology: &SystemTopology,
    open_object: &'a mut MaybeUninit<OpenObject>,
) -> Result<(Link, BpfSkel<'a>, Vec<Link>)> {
    // Increase MEMLOCK size since the BPF scheduler might use
    // more than the current limit
    try_set_rlimit_infinity();

    // Open the BPF prog first for verification.
    let mut skel_builder = BpfSkelBuilder::default();
    skel_builder.obj_builder.debug(verbose > 0);
    let mut skel = scx_ops_open!(skel_builder, open_object, finesched_ops, None)?;

    let data = skel
        .maps
        .data_data
        .as_deref_mut()
        .context("BPF .data map is unavailable")?;
    data.use_switch_kfunc = use_switch_kfunc;
    data.enable_task_ip_monitoring = task_ip_monitoring;
    data.nr_cpu_ids_config = topology.nr_cpu_ids;
    data.nr_online_cpus_config = topology.nr_online_cpus;
    let bss = skel
        .maps
        .bss_data
        .as_deref_mut()
        .context("BPF .bss map is unavailable")?;
    bss.last_online_cpu_config = topology.last_online_cpu;
    for cpu in &topology.cpus {
        let dst = &mut bss.cpu_topology[cpu.id as usize];
        dst.numa_node = cpu.numa_node;
        dst.package_id = cpu.package_id;
        dst.die_id = cpu.die_id;
        dst.cluster_id = cpu.cluster_id;
        dst.core_id = cpu.core_id;
        dst.capacity = cpu.capacity;
        dst.max_freq_khz = cpu.max_freq_khz;
        dst.core_type = cpu.core_type;
        dst.flags = cpu.flags;
        dst.smt_sibling_count = cpu.smt_sibling_count;
    }

    // reuse the pinned map
    assert!(reuse_pinned_map(&mut skel.maps.gMap, SCHED_GROUP_MAP_PATH));
    assert!(reuse_pinned_map(&mut skel.maps.cMap, CGROUP_MAP_PATH));
    assert!(reuse_pinned_map(&mut skel.maps.gStats, SCHED_GROUP_STATS_MAP_PATH));

    // load the scheduler
    let mut skel = scx_ops_load!(skel, finesched_ops, uei)?;

    // coreMetrics is produced by the scheduler rather than the control-plane
    // hashmap object, so pin it after loading for policy-side readers.
    rm_pinned_map(CORE_METRICS_MAP_PATH);
    pin_map(&mut skel.maps.coreMetrics, CORE_METRICS_MAP_PATH);

    let perf_hw_cycles_links = if task_ip_monitoring {
        attach_perf_hw_cycles_event(&mut skel, topology)?
    } else {
        Vec::new()
    };

    // Attach.
    let struct_ops = scx_ops_attach!(skel, finesched_ops)?;

    return Ok((struct_ops, skel, perf_hw_cycles_links));
}

// TODO: make this function idempotent
pub fn load_bpf_scheduler_async(
    verbose: u8,
    use_switch_kfunc: bool,
    task_ip_monitoring: bool,
) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let topology = SystemTopology::discover(crate::bpf_intf::consts_MAX_CPUS)
        .expect("failed to discover CPU topology for FineSched");
    println!(
        "FineSched topology: {} online CPUs in ID range 0..{}, {} big and {} little logical CPUs",
        topology.nr_online_cpus,
        topology.nr_cpu_ids - 1,
        topology.big_cpu_count(),
        topology.little_cpu_count()
    );

    let launched = Arc::new(AtomicBool::new(false));
    let launched_clone = launched.clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let h = thread::spawn(move || {
        // load the bpf scheduler
        // output any debug info during load
        let mut open_object = MaybeUninit::uninit();
        let (struct_ops, skel, perf_hw_cycles_links) = load_bpf_scheduler(
            verbose,
            use_switch_kfunc,
            task_ip_monitoring,
            &topology,
            &mut open_object,
        )
        .unwrap();
        launched_clone.store(true, Ordering::Relaxed);
        loop {
            if shutdown.load(Ordering::Relaxed) {
                drop(struct_ops); // kill the bpf scheduler
                while !uei_exited!(&skel, uei) {}
                uei_report!(&skel, uei);
                rm_pinned_map(CORE_METRICS_MAP_PATH);
                break;
            }

            if uei_exited!(&skel, uei) {
                uei_report!(&skel, uei);
            }

            thread::sleep(std::time::Duration::from_millis(1000));
        }
    });

    while !launched.load(Ordering::Relaxed) {
        thread::sleep(std::time::Duration::from_millis(100));
    }

    (shutdown_clone, h)
}
