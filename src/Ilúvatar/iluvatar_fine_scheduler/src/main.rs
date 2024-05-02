// Copyright (c) Andrea Righi <andrea.righi@canonical.com>

// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.
#[allow(unused_imports)]
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
use shmem_ipc::sharedring::Sender;
use std::time::Duration;

use serde::Deserialize;
use serde::de;
use serde_json;
use serde_json::{Value};

use std::collections::HashMap;
use std::thread;

struct Scheduler<'a> {
    bpf: BpfScheduler<'a>,
}

impl<'a> Scheduler<'a> {
    fn init() -> Result<Self> {
        let topo = Topology::new().expect("Failed to build host topology");
        let bpf = BpfScheduler::init(5000, topo.nr_cpus_possible() as i32, false, 0, false, false)?;
        Ok(Self { bpf })
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn dispatch_tasks(&mut self) {
        loop {
            // Get queued taks and dispatch them in order (FIFO).
            match self.bpf.dequeue_task() {
                Ok(Some(task)) => {
                    // task.cpu < 0 is used to to notify an exiting task, in this
                    // case we can simply ignore the task.
                    if task.cpu >= 0 {
                        let _ = self.bpf.dispatch_task(&DispatchedTask::new(&task));

                        // Give the task a chance to run and prevent overflowing the dispatch queue.
                        std::thread::yield_now();
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
    }

    fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<()> {
        let mut prev_ts = Self::now();

        while !shutdown.load(Ordering::Relaxed) && !self.bpf.exited() {
            self.dispatch_tasks();

            let curr_ts = Self::now();
            if curr_ts > prev_ts {
                self.print_stats();
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

fn get_file_s( path: &str ) -> File {
    let f = File::
        open( path )
        .expect("Failed to open file");
    f
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
    preferred_core: i32,
}

#[derive(Debug, Deserialize, Clone)]
struct RecordPid {
    pid: i32,
    func_name: String,
}

fn read_csv<T: de::DeserializeOwned>( filename: &str ) -> Vec<T> {
    let f = get_file_s( filename );
    let mut rdr = csv::Reader::from_reader(f);
    let mut data = Vec::new();
    for result in rdr.deserialize() {
        let record: T = result.unwrap();
        data.push(record);
    }
    data
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

    fn update(&mut self){
        let data: Vec<RecordChr> =  read_csv( &self.cfile );
        self.characteristics = vec_to_map_chr( data );
        
        let data_pids: Vec<RecordPid> = read_csv( &self.pfile );
        self.pids = vec_to_map_pid( data_pids );
    }
}

fn main() -> Result<()> {

    let mut fdata = FuncData::new( 
        "/data2/ar/workspace/finescheduling/iluvatar-faas/src/Ilúvatar/iluvatar_fine_scheduler/examples/characteristics.csv".to_string(),
        "/data2/ar/workspace/finescheduling/iluvatar-faas/src/Ilúvatar/iluvatar_fine_scheduler/examples/pids.log".to_string()
    );
    let mut fdata = Arc::new( Mutex::new(fdata) );
    fdata.lock().unwrap().update();
    
    println!("#############################################");
    println!("Read first");
    println!("{:?}", fdata);
    
    {
        let fdata = Arc::clone(&fdata);
        // create a thread to launch reach function in a separate thread 
        thread::spawn(move || {
            loop {
                println!("#### Reading in Thread ##########################");
                fdata.lock().unwrap().update();

                sleep(Duration::from_millis(1000));
            }
        });
    }

    {
        let fdata = Arc::clone(&fdata);
        // create a thread to launch reach function in a separate thread 
        thread::spawn(move || {
            loop {
                println!("#### Printing in Thread ##########################");
                println!("{:?}", fdata.lock().unwrap());

                sleep(Duration::from_millis(1000));
            }
        });
    }

    sleep( Duration::from_millis(5000) );
    
    print_warning();

    // Setup the ring buffer 
    let mut r = Sender::open(80 as usize, 
                        get_file_s("/memfd:f64 (deleted)"), 
                        get_file_s("anon_inode:[eventfd]"),
                        get_file_s("anon_inode:[eventfd]")
                )?;
    let mut items = 100000;

    loop {
        let item = 1.0f64 / (items as f64);
        r.send_raw( 
            |p: *mut f64, mut count| unsafe {
                if items < count { 
                    count = items 
                };
                for i in 0..count {
                    *p.offset( i as isize ) = item;
                }
                println!("Sending {} items of {}, in total {}", 
                        count,
                        item,
                        (count as f64)*item
                    );
                count
            }
        ).unwrap();
        items += 100000;
        sleep( Duration::from_millis(1000) );
    }

    let mut sched = Scheduler::init()?;
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
        let data: Vec<RecordChr> = super::read_csv( &"/data2/ar/workspace/finescheduling/iluvatar-faas/src/Ilúvatar/iluvatar_fine_scheduler/examples/characteristics.csv".to_string() );
        println!("{:?}", data);

        let map = super::vec_to_map_chr(data);
        println!("{:?}", map);

        let data_pids: Vec<RecordPid> = super::read_csv( &"/data2/ar/workspace/finescheduling/iluvatar-faas/src/Ilúvatar/iluvatar_fine_scheduler/examples/pids.log".to_string() );
        println!("{:?}", data_pids);

        let map_pids = super::vec_to_map_pid(data_pids);
        println!("{:?}", map_pids);
    }
}

