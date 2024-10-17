// Copyright (c) Andrea Righi <andrea.righi@linux.dev>

// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.
mod bpf_skel;
pub use bpf_skel::*;
pub mod bpf_intf;

mod bpf;
use bpf::*;

use scx_utils::UserExitInfo;

use libbpf_rs::OpenObject;

use std::mem::MaybeUninit;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use std::time::SystemTime;

use anyhow::Result;

use clap::Parser;

use serde::{Deserialize, Serialize};

use iluvatar_worker_library::services::containers::containerd::PidsPacket;
use iluvatar_worker_library::utils::characteristics_map::CharacteristicsPacket;
use iluvatar_worker_library::worker_api::Channels;
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use std::collections::HashMap;

use libc::{sched_param, sched_setscheduler};

// Defined in UAPI
const SCHED_EXT: i32 = 7;

#[derive(Debug, Deserialize, Serialize)]
pub struct ChannelsR {
    pub rx_chr: IpcReceiver<CharacteristicsPacket>,
    pub rx_pids: IpcReceiver<PidsPacket>,
}

struct Scheduler<'a> {
    bpf: BpfScheduler<'a>,
    characteristics: HashMap<String, CharacteristicsPacket>,
    pids: HashMap<u32, PidsPacket>,
    crecvs: Option<&'a mut ChannelsR>,
}

impl<'a> Scheduler<'a> {
    fn init(
        open_object: &'a mut MaybeUninit<OpenObject>,
        crecvs: Option<&'a mut ChannelsR>,
        slice_us: u64,
    ) -> Result<Self> {
        let bpf = BpfScheduler::init(
            open_object,
            slice_us,
            0,    // exit_dump_len (buffer size of exit info)
            true, // verbose (verbose output)
        )?;

        Ok(Self {
            bpf,
            characteristics: HashMap::new(),
            pids: HashMap::new(),
            crecvs,
        })
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn print_stats(&mut self) {
        println!("--------  Userspace thread running ----------");

        match self.bpf.dequeue_stats() {
            Ok(Some(stats)) => {
                println!("stats {:?}", stats);
            }
            Ok(None) | Err(_) => {}
        }

        if let Some(crecvs) = &self.crecvs {
            loop {
                match crecvs.rx_chr.try_recv() {
                    Ok(chr) => {
                        // Do something interesting with your result
                        //println!("Received characteristics");
                        //println!("{:?}", chr);
                        self.characteristics.insert(chr.fqdn.clone(), chr);
                    }
                    Err(_) => break,
                }
            }
            for (k, v) in &self.characteristics {
                println!("{}: {:?}", k, v);
            }
        }
    }

    fn drain_queued_pids(&mut self) {
        let param: sched_param = sched_param { sched_priority: 0 };
        loop {
            // Get queued task and dispatch them in order (FIFO).
            match self.bpf.dequeue_pid() {
                Ok(Some(pid)) => {
                    println!("calling setscheduler syscall on {:?}", pid);
                    unsafe { sched_setscheduler(pid, SCHED_EXT, &param as *const sched_param) };
                }
                Ok(None) | Err(_) => {
                    break;
                }
            }
        }
    }

    fn fetch_over_channel(&mut self) {
        if let Some(crecvs) = &self.crecvs {
            loop {
                match crecvs.rx_chr.try_recv() {
                    Ok(chr) => {
                        // Do something interesting with your result
                        //println!("Received characteristics");
                        println!("received over channel {:?}", chr);
                        self.characteristics.insert(chr.fqdn.clone(), chr);
                    }
                    Err(_) => break,
                }
            }
        }
    }

    fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<UserExitInfo> {
        let mut prev_ts = Self::now();

        while !shutdown.load(Ordering::Relaxed) && !self.bpf.exited() {
            let curr_ts = Self::now();
            if curr_ts > prev_ts {
                self.print_stats();
                self.drain_queued_pids();
                self.fetch_over_channel();

                prev_ts = curr_ts;
            }
        }

        self.bpf.shutdown_and_report()
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    server_name: Option<String>,

    #[arg(long, default_value_t = 1000)]
    time_slice_us: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Starting power of 2 scheduler");

    let mut crecvs;
    if let Some(server_name) = &args.server_name {
        let (c_tx, c_rx): (
            IpcSender<CharacteristicsPacket>,
            IpcReceiver<CharacteristicsPacket>,
        ) = ipc::channel().unwrap();
        let (p_tx, p_rx): (IpcSender<PidsPacket>, IpcReceiver<PidsPacket>) =
            ipc::channel().unwrap();
        let server_tx = IpcSender::connect(server_name.clone()).unwrap();
        server_tx
            .send(Channels {
                tx_chr: c_tx,
                tx_pids: p_tx,
            })
            .unwrap();
        crecvs = Some(ChannelsR {
            rx_chr: c_rx,
            rx_pids: p_rx,
        });
    } else {
        crecvs = None;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();
    ctrlc::set_handler(move || {
        shutdown_clone.store(true, Ordering::Relaxed);
    })?;

    let mut open_object = MaybeUninit::uninit();
    loop {
        let mut sched =
            Scheduler::init(&mut open_object, (&mut crecvs).into(), args.time_slice_us)?;
        if !sched.run(shutdown.clone())?.should_restart() {
            break;
        }
    }

    Ok(())
}
