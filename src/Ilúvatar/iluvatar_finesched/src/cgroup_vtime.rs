use anyhow::{Context, Result};
use libbpf_rs::{MapCore, MapFlags, MapHandle};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

pub const CGROUP_VTIME_MAP_PATH: &str = "/sys/fs/bpf/scx1/cgrp_ctx_map";

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CgroupVtimeContext {
    vtime: u64,
    inv_start_ns: u64,
    cpu_used_ns: u64,
    ewma_wall_ns: u64,
    ema_par_q8: u32,
    nr_runnable: u32,
    last_quiescent_ns: u64,
    lock_word: u32,
    reserved: u32,
    invocation_id: u64,
    pending_invocation_id: u64,
}

fn cgroup_dir(cgroup_id: &str) -> Result<PathBuf> {
    let candidates = [
        Path::new("/sys/fs/cgroup/scx.slice").join(cgroup_id),
        Path::new("/sys/fs/cgroup").join(cgroup_id),
    ];
    candidates
        .into_iter()
        .find(|path| path.join("cgroup.procs").is_file())
        .with_context(|| format!("Unable to locate cgroup directory for '{cgroup_id}'"))
}

/// Update the wallclock duration estimate for a container in FineSched's
/// pinned cgroup-storage map. The cgroup FD is the key required by
/// BPF_MAP_TYPE_CGRP_STORAGE.
pub fn update_cgroup_wall_estimate(
    cgroup_id: &str,
    ewma_wall_ns: u64,
    invocation_id: u64,
) -> Result<()> {
    let cgroup_path = cgroup_dir(cgroup_id)?;
    let cgroup_fd = File::open(&cgroup_path)
        .with_context(|| format!("Failed to open cgroup {}", cgroup_path.display()))?;
    let fd_key = cgroup_fd.as_raw_fd();
    let map = MapHandle::from_pinned_path(CGROUP_VTIME_MAP_PATH)
        .with_context(|| format!("Failed to open pinned map {CGROUP_VTIME_MAP_PATH}"))?;

    let mut context = map
        .lookup(&fd_key.to_ne_bytes(), MapFlags::ANY)
        .context("Failed to look up cgroup scheduling state")?
        .map(|bytes| {
            if bytes.len() != std::mem::size_of::<CgroupVtimeContext>() {
                anyhow::bail!("Unexpected cgroup scheduling state size: {}", bytes.len());
            }
            // The BPF spin lock is not exposed to userspace; copy the
            // surrounding fields and always write its ABI word as zero.
            let mut value = CgroupVtimeContext::default();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    bytes.as_ptr(),
                    &mut value as *mut CgroupVtimeContext as *mut u8,
                    bytes.len(),
                );
            }
            value.lock_word = 0;
            Ok(value)
        })
        .transpose()?
        .unwrap_or_else(|| CgroupVtimeContext { ema_par_q8: 256, ..Default::default() });

    context.ewma_wall_ns = ewma_wall_ns;
    context.pending_invocation_id = invocation_id;
    context.lock_word = 0;
    let value = unsafe {
        std::slice::from_raw_parts(
            &context as *const CgroupVtimeContext as *const u8,
            std::mem::size_of::<CgroupVtimeContext>(),
        )
    };
    map.update(&fd_key.to_ne_bytes(), value, MapFlags::ANY)
        .context("Failed to update cgroup wallclock estimate")
}
