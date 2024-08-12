// Copyright (c) Andrea Righi <andrea.righi@canonical.com>

// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.
#[allow(unused_imports)]
use clap::Parser;
mod bpf_skel;
pub use bpf_skel::*;
pub mod bpf_intf;

mod bpf;
use bpf::*;

use scx_utils::Topology;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::fs::File;
use std::fs::read_to_string;
#[cfg(any(unix, target_os = "wasi"))]
use std::os::fd::{FromRawFd};

use std::time::SystemTime;

use anyhow::Result;

use std::thread::sleep;
use std::time::Duration;

use serde::{Deserialize,Serialize};
use serde::de;
use serde_json;
use serde_json::{Value};

use std::collections::HashMap;
use std::thread;

use libc::{sched_param, sched_setscheduler};

use ipc_channel::ipc::{self, IpcOneShotServer, IpcSender, IpcReceiver};
use iluvatar_worker_library::worker_api::Channels;
use iluvatar_library::characteristics_map::CharacteristicsPacket;
use iluvatar_worker_library::services::containers::containerd::PidsPacket;

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
    crecvs: Option<ChannelsR>,
    cores: Vec<i32>,
}

impl<'a> Scheduler<'a> {
    fn init( 
            crecvs: Option<ChannelsR>,
            cores: Vec<i32>,
        ) -> Result<Self> {

        let bpf = BpfScheduler::init(
                     5000,  // slice us 
                     cores.clone(), 
                     true,  // partial 
                     0,     // exit_dump_len 
                 )?;

        Ok( Self { 
            bpf, 
            characteristics: HashMap::new(), 
            pids: HashMap::new(),
            crecvs,
            cores
        }) 
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn print_stats(&mut self) {
        let nr_tasks = *self.bpf.nr_tasks();
        let nr_eq_tasks = *self.bpf.nr_eq_tasks();

        println!(
            "total tasks init: {} enqueued in shared queue: {}",
            nr_tasks,
            nr_eq_tasks,
        );
        
        if let Some(crecvs) = &self.crecvs {
            loop {
                match crecvs.rx_chr.try_recv() {
                    Ok(chr) => {
                        // Do something interesting with your result
                        //println!("Received characteristics");
                        //println!("{:?}", chr);
                        self.characteristics.insert( chr.fqdn.clone(), chr );
                    },
                    Err(_) => break,
                }
            }
            let param: sched_param = sched_param { sched_priority: 0 };
            loop {
                match crecvs.rx_pids.try_recv() {
                    Ok(pids) => {
                        // Do something interesting with your result
                        //println!("Received pids");
                        //println!("{:?}", pids);
                        unsafe { sched_setscheduler(pids.pid as i32, SCHED_EXT, &param as *const sched_param) };
                        self.pids.insert( pids.pid, pids );
                    },
                    Err(_) => break,
                }
            }
            for (k, v) in &self.characteristics {
                println!("{}: {:?}", k, v);
            }
        }
    }

    fn drain_queued_pids(&mut self){
        let param: sched_param = sched_param { sched_priority: 0 };
        loop {
            // Get queued task and dispatch them in order (FIFO).
            match self.bpf.dequeue_pid() {
                Ok(Some(pid)) => {
                    println!("calling setscheduler syscall on {:?}", pid);
                    unsafe { sched_setscheduler(pid as i32, SCHED_EXT, &param as *const sched_param) };
                }
                Ok(None) | Err(_) => {
                    break;
                }
            }
        }
    }

    fn fetch_over_channel(&mut self){
        if let Some(crecvs) = &self.crecvs {
            loop {
                match crecvs.rx_chr.try_recv() {
                    Ok(chr) => {
                        // Do something interesting with your result
                        //println!("Received characteristics");
                        println!("received over channel {:?}", chr);
                        self.characteristics.insert( chr.fqdn.clone(), chr );
                    },
                    Err(_) => break,
                }
            }
        }
    }

    fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<()> {
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

    #[arg(short, long)]
    cores: Vec<i32>,
}


fn main() -> Result<()> {

    let args = Args::parse();

    println!("Constraining tasks to cores {:?}",args.cores);

    let crecvs;
    if let Some(server_name) = &args.server_name {
        let (c_tx, c_rx): (IpcSender<CharacteristicsPacket>, IpcReceiver<CharacteristicsPacket>) = ipc::channel().unwrap();
        let (p_tx, p_rx): (IpcSender<PidsPacket>, IpcReceiver<PidsPacket>) = ipc::channel().unwrap();
        let server_tx = IpcSender::connect(server_name.clone()).unwrap();
        server_tx.send( Channels{ tx_chr: c_tx, tx_pids: p_tx } ).unwrap();
        crecvs = Some(ChannelsR{ rx_chr: c_rx, rx_pids: p_rx });
    } else {
        crecvs = None;
    }

    let mut sched = Scheduler::init( crecvs, args.cores )?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    ctrlc::set_handler(move || {
        shutdown_clone.store(true, Ordering::Relaxed);
    })?;
    
    // wait for the worker to start the scheduler 
    sched.run(shutdown)
}


