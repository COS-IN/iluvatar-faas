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

use iluvatar_library::clock::get_global_timestamp_ms;

use anyhow::Result;

use clap::Parser;

use serde::{Deserialize, Serialize};

use iluvatar_worker_library::utils::characteristics_map::CharacteristicsPacket;
use iluvatar_worker_library::worker_api::fs_scheduler::Channels;

use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize)]
pub struct ChannelsR {
    pub rx_chr: IpcReceiver<CharacteristicsPacket>,
}

struct Scheduler<'a> {
    bpf: BpfScheduler<'a>,
    characteristics: HashMap<String, CharacteristicsPacket>,
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
            crecvs,
        })
    }

    fn now() -> i64 {
        get_global_timestamp_ms()
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
            while let Ok(chr) = crecvs.rx_chr.try_recv() {
                self.characteristics.insert(chr.fqdn.clone(), chr);
            }
            for (k, v) in &self.characteristics {
                println!("{}: {:?}", k, v);
            }
        }
    }

    fn fetch_over_channel(&mut self) {
        if let Some(crecvs) = &self.crecvs {
            while let Ok(chr) = crecvs.rx_chr.try_recv() {
                self.characteristics.insert(chr.fqdn.clone(), chr);
            }
        }
    }

    fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<UserExitInfo> {
        let mut prev_ts = Self::now();

        while !shutdown.load(Ordering::Relaxed) && !self.bpf.exited() {
            let curr_ts = Self::now();
            if curr_ts > prev_ts {
                self.print_stats();
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
        let server_tx = IpcSender::connect(server_name.clone()).unwrap();
        server_tx.send(Channels { tx_chr: c_tx }).unwrap();
        crecvs = Some(ChannelsR { rx_chr: c_rx });
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
