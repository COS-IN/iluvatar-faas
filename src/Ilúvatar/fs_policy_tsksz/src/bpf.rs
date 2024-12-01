// Copyright (c) Andrea Righi <andrea.righi@linux.dev>

// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use scx_utils::enums::scx_enums;
use scx_utils::import_enums;
use std::mem::MaybeUninit;

use crate::bpf_intf;
use crate::bpf_skel::*;

use anyhow::Context;
use anyhow::Result;

use libbpf_rs::skel::OpenSkel;
use libbpf_rs::skel::Skel;
use libbpf_rs::skel::SkelBuilder;
use libbpf_rs::OpenObject;

use libc::{pthread_self, pthread_setschedparam, sched_param};

#[cfg(target_env = "musl")]
use libc::timespec;

use scx_utils::scx_ops_attach;
use scx_utils::scx_ops_load;
use scx_utils::scx_ops_open;
use scx_utils::uei_exited;
use scx_utils::uei_report;
use scx_utils::UserExitInfo;

use scx_rustland_core::ALLOCATOR;

// Defined in UAPI
const SCHED_EXT: i32 = 7;

pub struct BpfScheduler<'cb> {
    pub skel: BpfSkel<'cb>,                   // Low-level BPF connector
    struct_ops: Option<libbpf_rs::Link>,      // Low-level BPF methods
    queued_stats: libbpf_rs::RingBuffer<'cb>, // ring buffer of tasks pids to be switched to schedext
}

#[derive(Clone, Copy, Debug)]
#[allow(non_camel_case_types, dead_code)]
pub struct lpolicy_stats(bpf_intf::policy_stats);

macro_rules! define_buffer {
    ( $bufname: ident, $abufname: ident, $abuf: ident, $callback: ident, $tdst: ty ) => {
        const $bufname: usize = std::mem::size_of::<$tdst>();
        #[repr(align(8))]
        struct $abufname([u8; $bufname]);
        static mut $abuf: $abufname = $abufname([0; $bufname]);
        fn $callback(data: &[u8]) -> i32 {
            unsafe {
                $abuf.0.copy_from_slice(data);
            }
            LIBBPF_STOP
        }
    };
}

define_buffer!(
    BUFSIZE_STATS,
    AlignedBufferstats,
    BUF_STATS,
    callback_stats,
    bpf_intf::policy_stats
);
fn fetch_stats(bytes: &[u8]) -> lpolicy_stats {
    let ps = unsafe { *(bytes.as_ptr() as *const bpf_intf::policy_stats) };
    lpolicy_stats(ps)
}

// Special negative error code for libbpf to stop after consuming just one item from a BPF
// ring buffer.
const LIBBPF_STOP: i32 = -255;

impl<'cb> BpfScheduler<'cb> {
    pub fn init(
        open_object: &'cb mut MaybeUninit<OpenObject>,
        slice_us: u64,
        exit_dump_len: u32,
        verbose: bool,
    ) -> Result<Self> {
        // Open the BPF prog first for verification.
        let mut skel_builder = BpfSkelBuilder::default();
        skel_builder.obj_builder.debug(verbose);
        let mut skel = scx_ops_open!(skel_builder, open_object, tsksz_ops)?;

        // Lock all the memory to prevent page faults that could trigger potential deadlocks during
        // scheduling.
        ALLOCATOR.lock_memory();

        skel.struct_ops.tsksz_ops_mut().exit_dump_len = exit_dump_len;
        skel.maps.bss_data.usersched_pid = std::process::id();
        skel.maps.rodata_data.effective_slice_ns = slice_us * 1000;

        let path = "/sys/fs/bpf/func_metadata";
        let func_metadata = &mut skel.maps.func_metadata;
        assert!(func_metadata.reuse_pinned_map("/asdf").is_err());
        func_metadata
            .reuse_pinned_map(path)
            .expect("failed to reuse map");

        // Attach BPF scheduler.
        let mut skel = scx_ops_load!(skel, tsksz_ops, uei)?;
        let struct_ops = Some(scx_ops_attach!(skel, tsksz_ops)?);

        // Build the ring buffer of queued tasks.
        let rb_map = &mut skel.maps.queued_stats;
        let mut builder = libbpf_rs::RingBufferBuilder::new();
        builder.add(rb_map, callback_stats).unwrap();
        let queued_stats = builder.build().unwrap();

        // Make sure to use the SCHED_EXT class at least for the scheduler itself.
        match Self::use_sched_ext() {
            0 => Ok(Self {
                skel,
                struct_ops,
                queued_stats,
            }),
            err => Err(anyhow::Error::msg(format!(
                "sched_setscheduler error: {}",
                err
            ))),
        }
    }

    // Receive stats from the BPF scheduler to switch to schedext policy.
    pub fn dequeue_stats(&mut self) -> Result<Option<lpolicy_stats>, i32> {
        match self.queued_stats.consume_raw() {
            0 => Ok(None),
            LIBBPF_STOP => {
                // A valid pid is received, convert data to a proper pid.
                let stats = unsafe { fetch_stats(&BUF_STATS.0) };
                Ok(Some(stats))
            }
            res if res < 0 => Err(res),
            res => panic!(
                "Unexpected return value from libbpf-rs::consume_raw(): {}",
                res
            ),
        }
    }

    // Set scheduling class for the scheduler itself to SCHED_EXT
    fn use_sched_ext() -> i32 {
        #[cfg(target_env = "gnu")]
        let param: sched_param = sched_param { sched_priority: 0 };
        #[cfg(target_env = "musl")]
        let param: sched_param = sched_param {
            sched_priority: 0,
            sched_ss_low_priority: 0,
            sched_ss_repl_period: timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            sched_ss_init_budget: timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            sched_ss_max_repl: 0,
        };

        unsafe { pthread_setschedparam(pthread_self(), SCHED_EXT, &param as *const sched_param) }
    }

    // Read exit code from the BPF part.
    pub fn exited(&mut self) -> bool {
        uei_exited!(&self.skel, uei)
    }

    // Called on exit to shutdown and report exit message from the BPF part.
    pub fn shutdown_and_report(&mut self) -> Result<UserExitInfo> {
        self.struct_ops.take();
        uei_report!(&self.skel, uei)
    }
}

// Disconnect the low-level BPF scheduler.
impl<'a> Drop for BpfScheduler<'a> {
    fn drop(&mut self) {
        if let Some(struct_ops) = self.struct_ops.take() {
            drop(struct_ops);
        }
        ALLOCATOR.unlock_memory();
    }
}
