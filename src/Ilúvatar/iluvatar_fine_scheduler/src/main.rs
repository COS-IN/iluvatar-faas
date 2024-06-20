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

use iluvatar_worker_library::worker_api::Channels;
use iluvatar_library::characteristics_map::CharacteristicsPacket;
use iluvatar_worker_library::services::containers::containerd::PidsPacket;

#[derive(Debug, Deserialize, Serialize)]
pub struct ChannelsR {
    pub rx_chr: IpcReceiver<CharacteristicsPacket>,
    pub rx_pids: IpcReceiver<PidsPacket>,
}

struct Scheduler<'a> {
    bpf: BpfScheduler<'a>,
    fdata: &'a Arc<Mutex<FuncData>>,
    func_to_cpu: HashMap<String, i32>,
    crecvs: ChannelsR,
}

impl<'a> Scheduler<'a> {
    fn init( 
            fdata: &'a Arc<Mutex<FuncData>>, 
            crecvs: ChannelsR,
        ) -> Result<Self> {

        let topo = Topology::new().expect("Failed to build host topology");
        let bpf = BpfScheduler::init(5000, topo.nr_cpus_possible() as i32, false, 0, false, true)?;
        let fcmap = get_func_to_cpu_map();
        Ok(Self { bpf, fdata, func_to_cpu: fcmap, crecvs } ) 
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn dispatch_tasks(&mut self) {
        loop {
            // Get queued task and dispatch them in order (FIFO).
            match self.bpf.dequeue_task() {
                Ok(Some(task)) => {

                    // task.cpu < 0 is used to to notify an exiting task, in this
                    // case we can simply ignore the task.
                    if task.cpu >= 0 {
                        let dtask = &mut DispatchedTask::new(&task);

                        let mut lock = self.fdata.try_lock();
                        if let Ok(ref mut fldata) = lock {

                            if let Some( chr ) = fldata.pids.get( &task.pid ) {
                                if let Some( cpu ) = self.func_to_cpu.get( chr ) {
                                    // println!("Dispatching task {} to CPU {}", task.pid, cpu);
                                    dtask.set_cpu( *cpu );
                                }
                            }

                            let _ = self.bpf.dispatch_task( dtask );

                            // Give the task a chance to run and prevent overflowing the dispatch queue.
                            std::thread::yield_now();
                        } 
                    }
                }
                Ok(None) => {
                    // Notify the BPF component that all tasks have been scheduled and dispatched.
                    self.bpf.update_tasks(Some(0), Some(0));
                    break;
                }
                Err(_) => {
                    break;
                }
            }
        }
        // All queued tasks have been dipatched, yield to reduce scheduler's CPU consumption.
        std::thread::yield_now();
    }

    fn print_stats(&mut self) {
        let nr_user_dispatches = *self.bpf.nr_user_dispatches_mut();
        let nr_kernel_dispatches = *self.bpf.nr_kernel_dispatches_mut();
        let nr_cancel_dispatches = *self.bpf.nr_cancel_dispatches_mut();
        let nr_bounce_dispatches = *self.bpf.nr_bounce_dispatches_mut();
        let nr_failed_dispatches = *self.bpf.nr_failed_dispatches_mut();
        let nr_sched_congested = *self.bpf.nr_sched_congested_mut();

        println!(
            "user={} kernel={} cancel={} bounce={} fail={} cong={}",
            nr_user_dispatches,
            nr_kernel_dispatches,
            nr_cancel_dispatches,
            nr_bounce_dispatches,
            nr_failed_dispatches,
            nr_sched_congested,
        );

        match self.crecvs.rx_chr.try_recv() {
            Ok(chr) => {
                // Do something interesting with your result
                println!("Received characteristics");
                println!("{:?}", chr);
            },
            Err(_) => {},
        }
        match self.crecvs.rx_pids.try_recv() {
            Ok(pids) => {
                // Do something interesting with your result
                println!("Received pids");
                println!("{:?}", pids);
            },
            Err(_) => {},
        }
    }

    fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<()> {
        let mut prev_ts = Self::now();

        while !shutdown.load(Ordering::Relaxed) && !self.bpf.exited() {
            self.dispatch_tasks();

            let curr_ts = Self::now();
            if curr_ts > prev_ts {
                self.print_stats();
                let mut lock = self.fdata.try_lock();
                if let Ok(ref mut fldata) = lock {
                    fldata.update();
                    let mut i = 0;
                    for (k, v) in &fldata.pids {
                        let pid = *k as u32;
                        self.bpf.set_epid(pid, i);
                        i += 1;
                    }
                    self.bpf.switch_active_epid();
                } 
                prev_ts = curr_ts;
            }
        }

        self.bpf.shutdown_and_report()
    }
}

fn print_warning() {
    let warning = r#"
**************************************************************************

WARNING: The purpose of scx_rlfifo is to provide a simple scheduler
implementation based on scx_rustland_core, and it is not intended for
use in production environments. If you want to run a scheduler that makes
decisions in user space, it is recommended to use *scx_rustland* instead.

Please do not open GitHub issues in the event of poor performance, or
scheduler eviction due to a runnable task timeout. However, if running this
scheduler results in a system crash or the entire system becoming unresponsive,
please open a GitHub issue.

**************************************************************************"#;

    println!("{}", warning);
}

fn get_file_s( path: &str ) -> Result<File,()> {
    if std::path::Path::new(&path).exists() {
        let f = File::
            open( path )
            .expect("Failed to open file");
        return Ok(f);
    } 
    Err(()) 
}

fn vec_to_map_chr( data: Vec<RecordChr> ) -> HashMap<String, RecordChr> {
    data.chunks_exact(1) // chunks_exact returns an iterator of slices
    .map(|chunk| (chunk[0].func_name.clone(), chunk[0].clone())) // map slices to tuples
    .collect::<HashMap<_, _>>() // collect into a hashmap
}

fn vec_to_map_pid( data: Vec<RecordPid> ) -> HashMap<i32, String> {
    data.chunks_exact(1) // chunks_exact returns an iterator of slices
    .map(|chunk| (chunk[0].pid.clone(), chunk[0].func_name.clone())) // map slices to tuples
    .collect::<HashMap<_, _>>() // collect into a hashmap
}

#[derive(Debug, Deserialize, Clone)]
struct RecordChr {
    func_name: String,
    e2e_time: f64,
}

#[derive(Debug, Deserialize, Clone)]
struct RecordPid {
    pid: i32,
    func_name: String,
}

fn read_csv<T: de::DeserializeOwned>( filename: &str ) -> Vec<T> {
    let f = get_file_s( filename );
    let mut data = Vec::new();
    match f {
        Ok(f) => { 
            let mut rdr = csv::Reader::from_reader(f);
            for result in rdr.deserialize() {
                let record: T = result.unwrap();
                data.push(record);
            }
            return data;
        }
        Err(_) => return data,
    }
}

#[derive(Debug, Clone)]
struct FuncData {
    characteristics: HashMap<String, RecordChr>,
    pids: HashMap<i32, String>,
    cfile: String, 
    pfile: String, 
}

impl FuncData {
    fn new( cfile: String, pfile: String ) -> Self {
        let data: Vec<RecordChr> =  read_csv( &cfile );
        let data_pids: Vec<RecordPid> = read_csv( &pfile );
        let chr = vec_to_map_chr( data );
        let pids = vec_to_map_pid( data_pids );

        Self {
            characteristics: chr,
            pids: pids,
            cfile: cfile,
            pfile: pfile,
        }
    }

    fn update(&mut self) {
        let data: Vec<RecordChr> =  read_csv( &self.cfile );
        self.characteristics = vec_to_map_chr( data );
        
        let data_pids: Vec<RecordPid> = read_csv( &self.pfile );
        self.pids = vec_to_map_pid( data_pids );
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    server_name: String,

    #[arg(short, long)]
    characteristics_file: String,

    #[arg(short, long)]
    pids_file: String,
}

use ipc_channel::ipc::{self, IpcOneShotServer, IpcSender, IpcReceiver};

fn main() -> Result<()> {

    let args = Args::parse();
    
    println!("Characterics would be read from {}!", args.characteristics_file);
    println!("Pids would be read from {}!", args.pids_file);

    let (c_tx, c_rx): (IpcSender<CharacteristicsPacket>, IpcReceiver<CharacteristicsPacket>) = ipc::channel().unwrap();
    let (p_tx, p_rx): (IpcSender<PidsPacket>, IpcReceiver<PidsPacket>) = ipc::channel().unwrap();
    let server_tx = IpcSender::connect(args.server_name).unwrap();
    server_tx.send( Channels{ tx_chr: c_tx, tx_pids: p_tx } ).unwrap();
    let crecvs = ChannelsR{ rx_chr: c_rx, rx_pids: p_rx };
    
    let mut fdata = FuncData::new(
        args.characteristics_file,
        args.pids_file
    );
    let mut fdata = Arc::new( Mutex::new(fdata) );
    fdata.lock().unwrap().update();

    print_warning();

    let mut sched = Scheduler::init( &fdata, crecvs )?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    ctrlc::set_handler(move || {
        shutdown_clone.store(true, Ordering::Relaxed);
    })?;
    
    // wait for the worker to start the scheduler 
    sched.run(shutdown)
}


#[cfg(test)]
mod tests {
    type RecordChr = super::RecordChr;
    type RecordPid = super::RecordPid;

    fn read_csvs() {
        let data: Vec<RecordChr> = super::read_csv( &"/tmp/iluvatar/csvs/characteristics.csv".to_string() );
        println!("{:?}", data);

        let map = super::vec_to_map_chr(data);
        println!("{:?}", map);

        let data_pids: Vec<RecordPid> = super::read_csv( &"/tmp/iluvatar/csvs/pids.log".to_string() );
        println!("{:?}", data_pids);

        let map_pids = super::vec_to_map_pid(data_pids);
        println!("{:?}", map_pids);
    }
}


