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

const BASE_CGROUP_DIR: &str = "/sys/fs/cgroup";
const DOCKER_LOC: &str = "cpu,cpuacct/docker";
const METRIC_SYS: &str = "cpuacct.usage_sys";
const METRIC_USR: &str = "cpuacct.usage_user";
const METRIC_PCPU_SYS: &str = "cpuacct.usage_percpu_sys";
const METRIC_PCPU_USR: &str = "cpuacct.usage_percpu_user";

#[derive(Debug, Clone)]
pub struct CGROUPReading {
    usr: u64,
    sys: u64,
    pcpu_usr: Vec<u64>,
    pcpu_sys: Vec<u64>,
}

pub fn build_path( cgroupid: &String, metric: &str  ) -> String {
    format!("{}/{}/{}*/{}",
        BASE_CGROUP_DIR,
        DOCKER_LOC, 
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

pub fn read_as_u64( cgroupid: &String, metric: &str ) -> u64 
{
    let path = build_path( &cgroupid, metric );
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

pub fn read_as_u64_vec( cgroupid: &String, metric: &str ) -> Vec<u64> 
{
    let path = build_path( &cgroupid, metric );
    for entry in glob(path.as_str()).expect("Failed to read glob pattern") {
        let _ = match entry {
            Ok(pathb) => {
                let r = read_to_string( &pathb.into_os_string().into_string().unwrap() ).unwrap();
                println!("{} -> {}", path, r);
                let nums = r.trim().split(" ");
                let mut rnums = vec![];
                for n in nums {
                    rnums.push( u64::from_str_radix( n.trim(), 10 ).unwrap_or(0) );
                }
                return rnums;
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
        usr: read_as_u64(&cgroupid, METRIC_USR),
        sys: read_as_u64(&cgroupid, METRIC_SYS),
        pcpu_usr: read_as_u64_vec(&cgroupid, METRIC_PCPU_USR), 
        pcpu_sys: read_as_u64_vec(&cgroupid, METRIC_PCPU_SYS), 
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
