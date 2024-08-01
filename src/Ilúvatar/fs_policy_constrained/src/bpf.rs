// Copyright (c) Andrea Righi <andrea.righi@canonical.com>

// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::bpf_intf;
use crate::bpf_skel::*;

use anyhow::Context;
use anyhow::Result;

use libbpf_rs::skel::OpenSkel as _;
use libbpf_rs::skel::SkelBuilder as _;

use libc::{sched_param, sched_setscheduler};

use scx_utils::compat;
use scx_utils::init_libbpf_logging;
use scx_utils::scx_ops_attach;
use scx_utils::scx_ops_load;
use scx_utils::uei_exited;
use scx_utils::uei_report;

use scx_rustland_core::ALLOCATOR;

use std::collections::HashMap;

// Defined in UAPI
const SCHED_EXT: i32 = 7;

pub struct BpfScheduler<'cb> {
    pub skel: BpfSkel<'cb>,              // Low-level BPF connector
    struct_ops: Option<libbpf_rs::Link>, // Low-level BPF methods
}

// Special negative error code for libbpf to stop after consuming just one item from a BPF
// ring buffer.
const LIBBPF_STOP: i32 = -255;

impl<'cb> BpfScheduler<'cb> {
    pub fn init(
        slice_us: u64,
        constrained_cores: Vec<i32>,
        partial: bool,
        exit_dump_len: u32,
    ) -> Result<Self> {
        // Open the BPF prog first for verification.
        let skel_builder = BpfSkelBuilder::default();
        init_libbpf_logging(None);
        let mut skel = skel_builder.open().context("Failed to open BPF program")?;

        // Lock all the memory to prevent page faults that could trigger potential deadlocks during
        // scheduling.
        ALLOCATOR.lock_memory();
        
        // set the cores to constrain   
        for c in constrained_cores {
            skel.rodata_mut().constrained_cores[c as usize] = 1;
        }

        // Set scheduler options (defined in the BPF part).
        if partial {
            skel.struct_ops.constrained_mut().flags |= *compat::SCX_OPS_SWITCH_PARTIAL;
        }
        skel.struct_ops.constrained_mut().exit_dump_len = exit_dump_len;

        skel.bss_mut().usersched_pid = std::process::id();
        skel.rodata_mut().slice_ns = slice_us * 1000;

        // Attach BPF scheduler.
        let mut skel = scx_ops_load!(skel, constrained, uei)?;
        let struct_ops = Some(scx_ops_attach!(skel, constrained)?);

        // Make sure to use the SCHED_EXT class at least for the scheduler itself.
        match Self::use_sched_ext() {
            0 => Ok(Self {
                skel,
                struct_ops,
            }),
            err => Err(anyhow::Error::msg(format!(
                "sched_setscheduler error: {}",
                err
            ))),
        }
    }

    // Counter of number of tasks.
    #[allow(dead_code)]
    pub fn nr_tasks(& self) -> & u64 {
        & self.skel.bss().nr_tasks
    }

    // Set scheduling class for the scheduler itself to SCHED_EXT
    fn use_sched_ext() -> i32 {
        let pid = std::process::id();
        let param: sched_param = sched_param { sched_priority: 0 };
        let res =
            unsafe { sched_setscheduler(pid as i32, SCHED_EXT, &param as *const sched_param) };
        res
    }

    // Read exit code from the BPF part.
    pub fn exited(&mut self) -> bool {
        uei_exited!(&self.skel, uei)
    }

    // Called on exit to shutdown and report exit message from the BPF part.
    pub fn shutdown_and_report(&mut self) -> Result<()> {
        self.struct_ops.take();
        uei_report!(&self.skel, uei)
    }
}

// Disconnect the low-level BPF scheduler.
impl<'a> Drop for BpfScheduler<'a> {
    fn drop(&mut self) {
        if let Some( struct_ops ) = self.struct_ops.take() {
            drop( struct_ops );
        }
        ALLOCATOR.unlock_memory();
    }
}
