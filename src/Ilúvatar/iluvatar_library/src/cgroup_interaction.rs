/// The purpose of this file is to read all the current information available for a given cgroup. 
///     both for cgroup v1 and v2 
///     including docker and containerd backend - whereever the given cgroupid is found 
use crate::{bail_error, nproc, threading, transaction::TransactionId};
use anyhow::Result;
use std::thread::JoinHandle;
use glob::glob;
use std::{
    fs::File,
    io::{BufRead, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{error, info};

// base cgroup mount location 
const BASE_CGROUP_DIR: &str = "/sys/fs/cgroup";

/* 
 v1 location and names  

    vec of u64?
         tasks
             11753
             11754
             11755
             11756

         cgroup.procs -  
            11646
            11673
            11674
            11675
    
    key-val pairs - 
         cpu.stat
            nr_periods 0
            nr_throttled 0
            throttled_time 0
*/
const DOCKER_LOC_V1: &str      = "cpu,cpuacct/docker";
const V1_METRIC_SYS: &str      = "cpuacct.usage_sys";
const V1_METRIC_USR: &str      = "cpuacct.usage_user";
const V1_METRIC_PCPU_SYS: &str = "cpuacct.usage_percpu_sys";
const V1_METRIC_PCPU_USR: &str = "cpuacct.usage_percpu_user";
const V1_METRIC_TASKS: &str    = "tasks";
const V1_METRIC_PROCS: &str    = "cgroup.procs";
const V1_METRIC_STAT_CPU: &str = "cpu.stat";

/* 
 v2 location and names 
     /sys/fs/cgroup/unified/docker/89e979a2b0e9fd30d9b469c48c59a7650640851559db603bc13faa70eb57576b/cgroup.events
    
    custom parsing 
         cpu.pressure  
            some avg10=0.00 avg60=0.00 avg300=0.00 total=22

         io.pressure
            some avg10=0.00 avg60=0.00 avg300=0.00 total=315303
            full avg10=0.00 avg60=0.00 avg300=0.00 total=315297

         memory.pressure
            some avg10=0.00 avg60=0.00 avg300=0.00 total=0
            full avg10=0.00 avg60=0.00 avg300=0.00 total=0

    vec of u64?

         cgroup.threads
            11752
            11753
            11754
            11755
            11756

         cgroup.procs
            11646
            11673
            11674
            11675

    key-val pairs - 

         cpu.stat
            usage_usec 84553241
            user_usec 25088308
            system_usec 59464932

*/
const DOCKER_LOC_V2: &str      = "unified/docker"; // /cgroupid/
const V2_METRIC_PRS_CPU: &str  = "cpu.pressure";
const V2_METRIC_PRS_IO: &str   = "io.pressure";
const V2_METRIC_PRS_MEM: &str  = "memory.pressure";
const V2_METRIC_STAT_CPU: &str = "cpu.stat";
const V2_METRIC_TASKS: &str  = "cgroup.threads";
const V2_METRIC_PROCS: &str    = "cgroup.procs";

#[derive(Debug, Clone)]
pub struct CGROUPReadingV2 {
    pub threads: Vec<u64>,
    pub procs: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct CGROUPReading {
    // v1 
    pub usr: u64,
    pub sys: u64,
    pub pcpu_usr: Vec<u64>,
    pub pcpu_sys: Vec<u64>,
    pub threads: Vec<u64>,
    pub procs: Vec<u64>,
    
    // v2 
    pub v2: CGROUPReadingV2,
}

pub fn build_path( cgroupid: &String, metric: &str, docker_loc: &str  ) -> String {
    format!("{}/{}/{}*/{}",
        BASE_CGROUP_DIR,
        docker_loc, 
        cgroupid,
        metric  
    )
}

fn read_to_string( path: &String ) -> Result<String> {
    let pth = Path::new(path);
    let mut opened = match File::open(&pth) {
        Ok(b) => b,
        Err(e) => {
            bail_error!(path=%path, "couldn't open path for reading");
        }
    };
    let mut buff = String::new();
    match opened.read_to_string(&mut buff) {
        Ok(_) => Ok(buff),
        Err(e) => {
            bail_error!(error=%e, "Unable to read cpu freq file into buffer")
        }
    }
}

pub fn read_as_u64( cgroupid: &String, metric: &str, docker_loc: &str ) -> u64 
{
    let path = build_path( &cgroupid, metric, docker_loc );
    for entry in glob(path.as_str()).expect("Failed to read glob pattern") {
        let _ = match entry {
            Ok(pathb) => {
                let r = read_to_string( &pathb.into_os_string().into_string().unwrap() ).unwrap();
                println!("{} -> {}", path, r);
                return u64::from_str_radix( r.trim(), 10 ).unwrap_or(0);
                ()
            }
            Err(e) => println!("{:?}", e),
        };
    }

    0
}

pub fn read_as_u64_vec( cgroupid: &String, metric: &str, docker_loc: &str ) -> Vec<u64> 
{
    let path = build_path( &cgroupid, metric, docker_loc );
    for entry in glob(path.as_str()).expect("Failed to read glob pattern") {
        let _ = match entry {
            Ok(pathb) => {
                let r = read_to_string( &pathb.into_os_string().into_string().unwrap() ).unwrap();
                println!("{} -> {}", path, r);
                let mut nums: Vec<u64> = r.trim().split(" ").map( |n| u64::from_str_radix( n.trim(), 10 ).unwrap_or(0) ).collect();
                if nums.len() == 1 {
                    nums = r.trim().split("\n").map( |n| u64::from_str_radix( n.trim(), 10 ).unwrap_or(0) ).collect();
                }
                return nums;
                ()
            }
            Err(e) => println!("{:?}", e),
        };
    }

    vec![] 
}

pub fn read_cgroup( cgroupid: String )
    -> Result<CGROUPReading> { 
    Ok(CGROUPReading{
        usr      : read_as_u64(&cgroupid     , V1_METRIC_USR      , DOCKER_LOC_V1) ,
        sys      : read_as_u64(&cgroupid     , V1_METRIC_SYS      , DOCKER_LOC_V1) ,
        pcpu_usr : read_as_u64_vec(&cgroupid , V1_METRIC_PCPU_USR , DOCKER_LOC_V1) ,
        pcpu_sys : read_as_u64_vec(&cgroupid , V1_METRIC_PCPU_SYS , DOCKER_LOC_V1) ,
        threads  : read_as_u64_vec(&cgroupid , V1_METRIC_TASKS    , DOCKER_LOC_V1) ,
        procs    : read_as_u64_vec(&cgroupid , V1_METRIC_PROCS    , DOCKER_LOC_V1) ,

        v2: CGROUPReadingV2{
            threads : read_as_u64_vec(&cgroupid , V2_METRIC_TASKS , DOCKER_LOC_V2) ,
            procs   : read_as_u64_vec(&cgroupid , V2_METRIC_PROCS , DOCKER_LOC_V2) ,
        }
    })
}

#[cfg(test)]
mod cgroup_interaction_tests {
    use crate::cgroup_interaction::*;
    
    #[test]
    fn test_cg_read(){
        let cgroup_id = "89e979a2b0e9fd3".to_string();
        let res = read_cgroup( cgroup_id );
        println!("{:?}", res);
        assert!(false); 
    }
}
