// Copyright (c) Andrea Righi <andrea.righi@canonical.com>

// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.

use crate::bpf_intf;
use crate::bpf_skel::*;

use anyhow::Context;
use anyhow::Result;

use libbpf_rs::skel::OpenSkel as _;
use libbpf_rs::skel::SkelBuilder as _;
use libbpf_rs::PrintLevel;
use libbpf_rs::skel::Skel;

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

// Buffer to store pids from the bpf scheduler 
// NOTE: make the buffer aligned to 64-bits to prevent misaligned dereferences when accessing the
// buffer using a pointer.
const BUFSIZE: usize = std::mem::size_of::<i32>();
#[repr(align(8))]
struct AlignedBuffer([u8; BUFSIZE]);
static mut BUF: AlignedBuffer = AlignedBuffer([0; BUFSIZE]);
fn fetch_pid( bytes: &[u8] ) -> i32 {
    let ps = unsafe { *(bytes.as_ptr() as *const bpf_intf::packet_pid) };
    ps.pid
}

// Special negative error code for libbpf to stop after consuming just one item from a BPF
// ring buffer.
const LIBBPF_STOP: i32 = -255;

pub struct BpfScheduler<'cb> {
    pub skel: BpfSkel<'cb>,              // Low-level BPF connector
    struct_ops: Option<libbpf_rs::Link>, // Low-level BPF methods
    queued_pids: libbpf_rs::RingBuffer<'cb>,  // ring buffer of tasks pids to be switched to schedext
                                         // policy 
}

impl<'cb> BpfScheduler<'cb> {
    pub fn init(
        slice_us: u64,
        constrained_cores: Vec<i32>,
        partial: bool,
        exit_dump_len: u32,
    ) -> Result<Self> {
        // Open the BPF prog first for verification.
        let mut skel_builder = BpfSkelBuilder::default();
        skel_builder.obj_builder.debug(true);
        // init_libbpf_logging(Some(PrintLevel::Debug));
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
        
        // see fifo policy for why it's safe - summary: user space thread is just one thread  
        fn callback(data: &[u8]) -> i32 {
            unsafe {
                BUF.0.copy_from_slice(data);
            }
            LIBBPF_STOP
        }
        // Build the ring buffer of queued tasks.
        let binding = skel.maps();
        let queued_ring_buffer = binding.queued_pids();
        let mut rbb = libbpf_rs::RingBufferBuilder::new();
        rbb.add(queued_ring_buffer, callback)
            .expect("failed to add ringbuf callback");
        let queued_pids = rbb.build().expect("failed to build ringbuf");

        // Make sure to use the SCHED_EXT class at least for the scheduler itself.
        match Self::use_sched_ext() {
            0 => Ok(Self {
                skel,
                struct_ops,
                queued_pids,
            }),
            err => Err(anyhow::Error::msg(format!(
                "sched_setscheduler error: {}",
                err
            ))),
        }
    }

    // Receive a task pid from the BPF scheduler to switch to schedext policy.
    pub fn dequeue_pid(&mut self) -> Result<Option<i32>, i32> {
        match self.queued_pids.consume_raw() {
            0 => Ok(None),
            LIBBPF_STOP => {
                // A valid pid is received, convert data to a proper pid.
                let pid = unsafe { fetch_pid(&BUF.0) };
                Ok(Some(pid))
            }
            res if res < 0 => Err(res),
            res => panic!("Unexpected return value from libbpf-rs::consume_raw(): {}", res),
        }
    }

    // Counter of number of tasks.
    #[allow(dead_code)]
    pub fn nr_tasks(& self) -> & u64 {
        & self.skel.bss().nr_tasks
    }

    // Counter of number of tasks.
    #[allow(dead_code)]
    pub fn nr_eq_tasks(& self) -> & u64 {
        & self.skel.bss().nr_eq_tasks
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
