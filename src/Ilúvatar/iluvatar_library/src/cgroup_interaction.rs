/// The purpose of this file is to read all the current information available for a given cgroup.
///     both for cgroup v1 and v2
///     including docker and containerd backend - whereever the given cgroupid is found
use crate::bail_error;
use anyhow::Result;
use array_tool::vec::Union;
use glob::glob;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::{fs::File, io::Read, path::Path};

// base cgroup mount location
const BASE_CGROUP_DIR: &str = "/sys/fs/cgroup";

/*
 v1 location and names

    vec of u64?
         tasks                ✓
             11753
             11754
             11755
             11756

         cgroup.procs -                  ✓
            11646
            11673
            11674
            11675

    key-val pairs -
         cpu.stat                ✓
            nr_periods 0
            nr_throttled 0
            throttled_time 0
*/
const DOCKER_LOC_V1: &str = "cpu,cpuacct/docker";
const V1_METRIC_SYS: &str = "cpuacct.usage_sys";
const V1_METRIC_USR: &str = "cpuacct.usage_user";
const V1_METRIC_PCPU_SYS: &str = "cpuacct.usage_percpu_sys";
const V1_METRIC_PCPU_USR: &str = "cpuacct.usage_percpu_user";
const V1_METRIC_TASKS: &str = "tasks";
const V1_METRIC_PROCS: &str = "cgroup.procs";
const V1_METRIC_STAT_CPU: &str = "cpu.stat";

// Keys
pub const KEY_NR_PERIODS: &str = "nr_periods";
pub const KEY_NR_THROTTLED: &str = "nr_throttled";
pub const KEY_THROTTLED_TIME: &str = "throttled_time";

pub const KEY_USER_USEC: &str = "user_usec";
pub const KEY_SYSTEM_USEC: &str = "system_usec";
pub const KEY_USAGE_USEC: &str = "usage_usec";

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

         cgroup.threads                ✓
            11752
            11753
            11754
            11755
            11756

         cgroup.procs                ✓
            11646
            11673
            11674
            11675

    key-val pairs -

         cpu.stat                ✓
            usage_usec 84553241
            user_usec 25088308
            system_usec 59464932

*/
const DOCKER_LOC_V2: &str = "unified/docker"; // /cgroupid/
const V2_METRIC_PRS_CPU: &str = "cpu.pressure";
const V2_METRIC_PRS_IO: &str = "io.pressure";
const V2_METRIC_PRS_MEM: &str = "memory.pressure";
const V2_METRIC_STAT_CPU: &str = "cpu.stat";
const V2_METRIC_TASKS: &str = "cgroup.threads";
const V2_METRIC_PROCS: &str = "cgroup.procs";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CGROUPV2PsiVal {
    pub avg10: f32, // % of time group waited for the resource to become available - on average
    // over last 10 seconds
    pub avg60: f32,
    pub avg300: f32,
    pub total: u64, // accumulated useconds of stall
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CGROUPV2Psi {
    pub some: CGROUPV2PsiVal,
    pub full: CGROUPV2PsiVal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CGROUPReadingV2 {
    pub threads: Vec<u64>,
    pub procs: Vec<u64>,
    pub cpustats: HashMap<String, u64>,
    pub cpupsi: CGROUPV2Psi,
    pub mempsi: CGROUPV2Psi,
    pub iopsi: CGROUPV2Psi,
}

// at some point implement serde serialize and deserialize for this structure
// https://serde.rs/impl-serialize.html
// it would help convert this structure to a json or a csv very easily
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CGROUPReading {
    // v1
    pub usr: u64,
    pub sys: u64,
    pub pcpu_usr: Vec<u64>,
    pub pcpu_sys: Vec<u64>,
    pub threads: Vec<u64>,
    pub procs: Vec<u64>,
    pub cpustats: HashMap<String, u64>,

    // v2
    pub v2: CGROUPReadingV2,
}

impl Default for CGROUPReading {
    fn default() -> Self {
        CGROUPReading {
            usr: 0,
            sys: 0,
            pcpu_usr: vec![0, 0],
            pcpu_sys: vec![0, 0],
            threads: vec![],
            procs: vec![],
            cpustats: [
                (KEY_NR_PERIODS.to_string(), 0),
                (KEY_NR_THROTTLED.to_string(), 0),
                (KEY_THROTTLED_TIME.to_string(), 0),
            ]
            .iter()
            .cloned()
            .collect(),

            v2: CGROUPReadingV2 {
                threads: vec![],
                procs: vec![],
                cpustats: [
                    (KEY_NR_PERIODS.to_string(), 0),
                    (KEY_NR_THROTTLED.to_string(), 0),
                    (KEY_THROTTLED_TIME.to_string(), 0),
                ]
                .iter()
                .cloned()
                .collect(),
                cpupsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 0,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 0,
                    },
                },
                mempsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 0,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 0,
                    },
                },
                iopsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 0,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 0,
                    },
                },
            },
        }
    }
}

pub fn build_path(cgroupid: &String, metric: &str, docker_loc: &str) -> String {
    format!("{}/{}/{}*/{}", BASE_CGROUP_DIR, docker_loc, cgroupid, metric)
}

fn read_to_string(path: &String) -> Result<String> {
    let pth = Path::new(path);
    let mut opened = match File::open(pth) {
        Ok(b) => b,
        Err(_e) => {
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

fn read_to_string_first_match(cgroupid: &String, metric: &str, docker_loc: &str) -> Option<String> {
    let path = build_path(cgroupid, metric, docker_loc);
    for entry in glob(path.as_str()).expect("Failed to read glob pattern") {
        match entry {
            Ok(pathb) => match read_to_string(&pathb.into_os_string().into_string().unwrap()) {
                Ok(r) => return Some(r),
                Err(_) => return None,
            },
            Err(e) => println!("{:?}", e),
        };
    }
    None
}

pub fn read_as_u64(cgroupid: &String, metric: &str, docker_loc: &str) -> u64 {
    match read_to_string_first_match(cgroupid, metric, docker_loc) {
        Some(r) => {
            //println!("{} -> {}", cgroupid, r);
            return r.trim().parse::<u64>().unwrap_or(0);
        }
        None => 0,
    }
}

pub fn read_as_u64_vec(cgroupid: &String, metric: &str, docker_loc: &str) -> Vec<u64> {
    match read_to_string_first_match(cgroupid, metric, docker_loc) {
        Some(r) => {
            let mut nums: Vec<u64> = r
                .trim()
                .split(" ")
                .map(|n| n.trim().parse::<u64>().unwrap_or(0))
                .collect();
            if nums.len() == 1 {
                nums = r
                    .trim()
                    .split("\n")
                    .map(|n| n.trim().parse::<u64>().unwrap_or(0))
                    .collect();
            }
            nums
        }
        None => vec![],
    }
}

pub fn read_as_u64_hashmap(cgroupid: &String, metric: &str, docker_loc: &str) -> HashMap<String, u64> {
    match read_to_string_first_match(cgroupid, metric, docker_loc) {
        Some(r) => {
            let pairs: Vec<&str> = r.trim().split("\n").collect();
            let tuples: Vec<Vec<&str>> = pairs.iter().map(|x| x.trim().split(" ").collect()).collect();
            let mut result = HashMap::<String, u64>::new();
            for pair in tuples.iter() {
                result.insert(pair[0].to_string(), pair[1].trim().parse::<u64>().unwrap_or(0));
            }
            result
        }
        None => HashMap::new(),
    }
}

#[allow(non_snake_case)]
pub fn read_as_CGROUPV2Psi(cgroupid: &String, metric: &str, docker_loc: &str) -> CGROUPV2Psi {
    let mut data = CGROUPV2Psi {
        some: CGROUPV2PsiVal {
            avg10: 0.0,
            avg60: 0.0,
            avg300: 0.0,
            total: 0,
        },
        full: CGROUPV2PsiVal {
            avg10: 0.0,
            avg60: 0.0,
            avg300: 0.0,
            total: 0,
        },
    };

    match read_to_string_first_match(cgroupid, metric, docker_loc) {
        Some(r) => {
            //println!("check->{:?}", r);
            let lines: Vec<&str> = r.trim().split("\n").collect();
            let fillval = |line: &str, val: &mut CGROUPV2PsiVal| {
                // given line: "some avg10=0.00 avg60=0.00 avg300=0.00 total=22\n"
                let stuff = &line.split(" ").collect::<Vec<&str>>()[1..];
                let pairs: Vec<Vec<&str>> = stuff.iter().map(|x| x.split("=").collect()).collect();
                val.avg10 = f32::from_str(pairs[0][1]).unwrap_or(0.0);
                val.avg60 = f32::from_str(pairs[1][1]).unwrap_or(0.0);
                val.avg300 = f32::from_str(pairs[2][1]).unwrap_or(0.0);
                val.total = pairs[3][1].parse::<u64>().unwrap_or(0);
            };
            // fill in some
            fillval(lines[0], &mut data.some);
            if lines.len() == 2 {
                // let's fill in full as well
                fillval(lines[1], &mut data.full);
            }
            //println!("check->{:?}", lines);
            data
        }

        None => data,
    }
}

// TODO: remove hardcoding for docker location
// TODO: dynamic location for cgroup (it's different in case of containerd and docker)
pub fn read_cgroup(cgroupid: String) -> Result<CGROUPReading> {
    Ok(CGROUPReading {
        usr: read_as_u64(&cgroupid, V1_METRIC_USR, DOCKER_LOC_V1),
        sys: read_as_u64(&cgroupid, V1_METRIC_SYS, DOCKER_LOC_V1),
        pcpu_usr: read_as_u64_vec(&cgroupid, V1_METRIC_PCPU_USR, DOCKER_LOC_V1),
        pcpu_sys: read_as_u64_vec(&cgroupid, V1_METRIC_PCPU_SYS, DOCKER_LOC_V1),
        threads: read_as_u64_vec(&cgroupid, V1_METRIC_TASKS, DOCKER_LOC_V1),
        procs: read_as_u64_vec(&cgroupid, V1_METRIC_PROCS, DOCKER_LOC_V1),
        cpustats: read_as_u64_hashmap(&cgroupid, V1_METRIC_STAT_CPU, DOCKER_LOC_V1),

        v2: CGROUPReadingV2 {
            threads: read_as_u64_vec(&cgroupid, V2_METRIC_TASKS, DOCKER_LOC_V2),
            procs: read_as_u64_vec(&cgroupid, V2_METRIC_PROCS, DOCKER_LOC_V2),
            cpustats: read_as_u64_hashmap(&cgroupid, V2_METRIC_STAT_CPU, DOCKER_LOC_V2),
            cpupsi: read_as_CGROUPV2Psi(&cgroupid, V2_METRIC_PRS_CPU, DOCKER_LOC_V2),
            mempsi: read_as_CGROUPV2Psi(&cgroupid, V2_METRIC_PRS_MEM, DOCKER_LOC_V2),
            iopsi: read_as_CGROUPV2Psi(&cgroupid, V2_METRIC_PRS_IO, DOCKER_LOC_V2),
        },
    })
}

/// val1 - val0 -> result
pub fn diff_cgroupreading(val0: &CGROUPReading, val1: &CGROUPReading) -> CGROUPReading {
    fn diff_vec(v0: &[u64], v1: &[u64]) -> Vec<u64> {
        let mut diffvec = Vec::<u64>::new();
        if v0.len() != v1.len() {
            return vec![];
        }
        for (i, v) in v0.iter().enumerate() {
            diffvec.push(v1[i] - v);
        }
        diffvec
    }

    fn diff_hashmap(v0: &HashMap<String, u64>, v1: &HashMap<String, u64>) -> HashMap<String, u64> {
        let mut diffmap = HashMap::<String, u64>::new();
        for (k, v) in v0.iter() {
            if let Some(o) = v1.get(k) {
                diffmap.insert(k.clone(), o - v);
            }
        }
        diffmap
    }

    fn avg_psi(v0: &CGROUPV2PsiVal, v1: &CGROUPV2PsiVal) -> CGROUPV2PsiVal {
        let avg = |a, b| (a + b) / 2.0;
        CGROUPV2PsiVal {
            avg10: avg(v1.avg10, v0.avg10),
            avg60: avg(v1.avg60, v0.avg60),
            avg300: avg(v1.avg300, v0.avg300),
            total: v1.total - v0.total,
        }
    }

    CGROUPReading {
        usr: (val1.usr - val0.usr),
        sys: (val1.sys - val0.sys),
        pcpu_usr: diff_vec(&val0.pcpu_usr, &val1.pcpu_usr),
        pcpu_sys: diff_vec(&val0.pcpu_sys, &val1.pcpu_sys),
        threads: val1.threads.union(val0.threads.clone()),
        procs: val1.procs.union(val0.procs.clone()),
        cpustats: diff_hashmap(&val0.cpustats, &val1.cpustats),
        v2: CGROUPReadingV2 {
            threads: val1.v2.threads.union(val0.v2.threads.clone()),
            procs: val1.v2.procs.union(val0.v2.procs.clone()),
            cpustats: diff_hashmap(&val0.v2.cpustats, &val1.v2.cpustats),
            cpupsi: CGROUPV2Psi {
                some: avg_psi(&val0.v2.cpupsi.some, &val1.v2.cpupsi.some),
                full: avg_psi(&val0.v2.cpupsi.full, &val1.v2.cpupsi.full),
            },
            mempsi: CGROUPV2Psi {
                some: avg_psi(&val0.v2.mempsi.some, &val1.v2.mempsi.some),
                full: avg_psi(&val0.v2.mempsi.full, &val1.v2.mempsi.full),
            },
            iopsi: CGROUPV2Psi {
                some: avg_psi(&val0.v2.iopsi.some, &val1.v2.iopsi.some),
                full: avg_psi(&val0.v2.iopsi.full, &val1.v2.iopsi.full),
            },
        },
    }
}

#[cfg(test)]
mod cgroup_interaction_tests {
    use crate::cgroup_interaction::*;

    #[test]
    fn test_cg_diff() {
        let v0 = CGROUPReading {
            usr: 20,
            sys: 20,
            pcpu_usr: vec![20, 30],
            pcpu_sys: vec![20, 30],
            threads: vec![120, 130],
            procs: vec![120, 130],
            cpustats: [
                (KEY_NR_PERIODS.to_string(), 0),
                (KEY_NR_THROTTLED.to_string(), 0),
                (KEY_THROTTLED_TIME.to_string(), 0),
            ]
            .iter()
            .cloned()
            .collect(),

            v2: CGROUPReadingV2 {
                threads: vec![120, 130],
                procs: vec![120, 130],
                cpustats: [
                    (KEY_NR_PERIODS.to_string(), 0),
                    (KEY_NR_THROTTLED.to_string(), 0),
                    (KEY_THROTTLED_TIME.to_string(), 0),
                ]
                .iter()
                .cloned()
                .collect(),
                cpupsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 123,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 123,
                    },
                },
                mempsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 123,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 123,
                    },
                },
                iopsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 123,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.00,
                        avg60: 0.00,
                        avg300: 0.00,
                        total: 123,
                    },
                },
            },
        };
        let v1 = CGROUPReading {
            usr: 30,
            sys: 30,
            pcpu_usr: vec![30, 40],
            pcpu_sys: vec![30, 40],
            threads: vec![121, 131],
            procs: vec![121, 131],
            cpustats: [
                (KEY_NR_PERIODS.to_string(), 1),
                (KEY_NR_THROTTLED.to_string(), 1),
                (KEY_THROTTLED_TIME.to_string(), 1),
            ]
            .iter()
            .cloned()
            .collect(),

            v2: CGROUPReadingV2 {
                threads: vec![121, 131],
                procs: vec![121, 131],
                cpustats: [
                    (KEY_NR_PERIODS.to_string(), 1),
                    (KEY_NR_THROTTLED.to_string(), 1),
                    (KEY_THROTTLED_TIME.to_string(), 1),
                ]
                .iter()
                .cloned()
                .collect(),
                cpupsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.20,
                        avg60: 0.20,
                        avg300: 0.20,
                        total: 124,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.20,
                        avg60: 0.20,
                        avg300: 0.20,
                        total: 124,
                    },
                },
                mempsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.20,
                        avg60: 0.20,
                        avg300: 0.20,
                        total: 124,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.20,
                        avg60: 0.20,
                        avg300: 0.20,
                        total: 124,
                    },
                },
                iopsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.20,
                        avg60: 0.20,
                        avg300: 0.20,
                        total: 124,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.20,
                        avg60: 0.20,
                        avg300: 0.20,
                        total: 124,
                    },
                },
            },
        };

        let shouldbe = CGROUPReading {
            usr: 10,
            sys: 10,
            pcpu_usr: vec![10, 10],
            pcpu_sys: vec![10, 10],
            threads: vec![121, 131, 120, 130],
            procs: vec![121, 131, 120, 130],
            cpustats: [
                (KEY_NR_PERIODS.to_string(), 1),
                (KEY_NR_THROTTLED.to_string(), 1),
                (KEY_THROTTLED_TIME.to_string(), 1),
            ]
            .iter()
            .cloned()
            .collect(),

            v2: CGROUPReadingV2 {
                threads: vec![121, 131, 120, 130],
                procs: vec![121, 131, 120, 130],
                cpustats: [
                    (KEY_NR_PERIODS.to_string(), 1),
                    (KEY_NR_THROTTLED.to_string(), 1),
                    (KEY_THROTTLED_TIME.to_string(), 1),
                ]
                .iter()
                .cloned()
                .collect(),
                cpupsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.10,
                        avg60: 0.10,
                        avg300: 0.10,
                        total: 1,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.10,
                        avg60: 0.10,
                        avg300: 0.10,
                        total: 1,
                    },
                },
                mempsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.10,
                        avg60: 0.10,
                        avg300: 0.10,
                        total: 1,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.10,
                        avg60: 0.10,
                        avg300: 0.10,
                        total: 1,
                    },
                },
                iopsi: CGROUPV2Psi {
                    some: CGROUPV2PsiVal {
                        avg10: 0.10,
                        avg60: 0.10,
                        avg300: 0.10,
                        total: 1,
                    },
                    full: CGROUPV2PsiVal {
                        avg10: 0.10,
                        avg60: 0.10,
                        avg300: 0.10,
                        total: 1,
                    },
                },
            },
        };

        let diff = diff_cgroupreading(&v0, &v1);
        assert_eq!(diff, shouldbe);
    }
}
