use dashmap::DashMap;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::hashmaps_safe::SharedMapsRef;
use crate::CgroupChrs;
use crate::SchedGroupChrs;
use crate::SchedGroupID;

use crate::bpf_intf::*;
use crate::utils::vec_to_cpumask;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct SchedGroup {
    pub cores: Vec<u32>,
    pub ts: u64,
    pub fifo: u32,
    pub prio: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct PreallocGroupsConfig {
    /// Number of FineSched domains to generate from the online CPU topology.
    pub domain_count: usize,
    /// Timeslice applied to generated domains, in milliseconds.
    pub timeslice_ms: u64,
    pub groups: Vec<SchedGroup>,
}

impl Default for PreallocGroupsConfig {
    fn default() -> Self {
        Self {
            domain_count: 4,
            timeslice_ms: 5,
            groups: Vec::new(),
        }
    }
}

impl PreallocGroupsConfig {
    /// Build an even domain partition from online CPU topology. SMT siblings
    /// are grouped by package/core before physical cores are divided.
    pub fn generate_from_online_cpus(&self) -> anyhow::Result<Self> {
        let cpu_root = Path::new("/sys/devices/system/cpu");
        let mut physical_cores: BTreeMap<(u32, u32), Vec<u32>> = BTreeMap::new();

        for entry in fs::read_dir(cpu_root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(cpu) = name.strip_prefix("cpu").and_then(|id| id.parse::<u32>().ok()) else {
                continue;
            };

            let online_path = entry.path().join("online");
            if fs::read_to_string(&online_path).is_ok_and(|state| state.trim() == "0") {
                continue;
            }

            let topology = entry.path().join("topology");
            let package_id = Self::read_topology_id(&topology.join("physical_package_id"))?;
            let core_id = Self::read_topology_id(&topology.join("core_id"))?;
            physical_cores.entry((package_id, core_id)).or_default().push(cpu);
        }

        let mut cores: Vec<Vec<u32>> = physical_cores.into_values().collect();
        for siblings in &mut cores {
            siblings.sort_unstable();
        }
        if cores.is_empty() {
            anyhow::bail!(
                "no online CPUs with package/core topology found under {}",
                cpu_root.display()
            );
        }

        let requested_domains = if self.domain_count == 0 { 4 } else { self.domain_count };
        let domain_count = requested_domains.min(cores.len());
        if domain_count < requested_domains {
            tracing::warn!(
                requested_domains,
                physical_cores = cores.len(),
                actual_domains = domain_count,
                "[finesched] reducing domain count because there are fewer physical cores than requested"
            );
        }

        let template = self.groups.first();
        let default_prio = "arrival".to_string();
        let mut groups: Vec<SchedGroup> = (0..domain_count)
            .map(|_| SchedGroup {
                cores: Vec::new(),
                ts: self.timeslice_ms,
                fifo: template.map_or(0, |group| group.fifo),
                prio: template
                    .map(|group| group.prio.clone())
                    .filter(|prio| !prio.is_empty())
                    .unwrap_or_else(|| default_prio.clone()),
            })
            .collect();

        let base_cores_per_domain = cores.len() / domain_count;
        let extra_cores = cores.len() % domain_count;
        let mut next_core = 0;
        for (domain_id, group) in groups.iter_mut().enumerate() {
            let assigned_core_count = base_cores_per_domain + if domain_id < extra_cores { 1 } else { 0 };
            for core_siblings in &cores[next_core..next_core + assigned_core_count] {
                group.cores.extend(core_siblings.iter().copied());
            }
            next_core += assigned_core_count;
        }

        Ok(Self {
            domain_count,
            timeslice_ms: self.timeslice_ms,
            groups,
        })
    }

    fn read_topology_id(path: &Path) -> anyhow::Result<u32> {
        Ok(fs::read_to_string(path)?.trim().parse()?)
    }
}

/*
PreallocatedGroups
   * it's intended for one time use to preallocate the groups at init
   * there it's not threadsafe
*/

// preallocated groups
// thread safe, can pass around Arc<PreAllocatedGroups>
pub struct PreAllocatedGroups {
    sm: SharedMapsRef,
    // caution: do not return any mutable reference to elements (avoids deadlock)
    // this struct is thread safe
    groups: Arc<DashMap<SchedGroupID, SchedGroup>>,
    cid_map: DashMap<String, CgroupChrs>,
}

unsafe impl Sync for PreAllocatedGroups {}

fn enqprio_name_to_val(prio: &str) -> u32 {
    match prio {
        "arrival" => QEnqPrioType_QEnqPrioArrival,
        "srptover" => QEnqPrioType_QEnqPrioSRPTover,
        "srptreset" => QEnqPrioType_QEnqPrioSRPTreset,
        "shrtdur" => QEnqPrioType_QEnqPrioSHRTDUR,
        "shrtduruw" => QEnqPrioType_QEnqPrioSHRTDURUW,
        "plain" => QEnqPrioType_QEnqPrioPLAIN,
        "taskcount" => QEnqPrioType_QEnqPrioTaskCount,
        "invoc" => QEnqPrioType_QEnqPrioINVOC,
        _ => QEnqPrioType_QEnqPrioUndef,
    }
}

impl PreAllocatedGroups {
    pub fn new(sm: SharedMapsRef, groups: PreallocGroupsConfig) -> PreAllocatedGroups {
        // build hashmap of groups
        let gh: DashMap<_, _> = groups
            .groups
            .into_iter()
            .enumerate()
            .map(|x| {
                let (a, b) = x;
                (a as SchedGroupID, b)
            })
            .collect();

        // populate the sharedmap with it
        gh.iter().for_each(|ent| {
            let (gid, group) = (ent.key(), ent.value());
            // bpf scheduler ignores the reserved cores in current implementation
            let reserved_start = 0 as usize;
            let reserved_end = group.cores.len() / 3;
            let regular_start = 0;
            let regular_end = group.cores.len();

            let sg = SchedGroupChrs {
                id: *gid,
                reserved_corebitmask: vec_to_cpumask(&group.cores[reserved_start..reserved_end]),
                corebitmask: vec_to_cpumask(&group.cores[regular_start..regular_end]),
                core_count: group.cores.len() as u64,
                timeslice: group.ts, // in ms
                fifo: group.fifo,
                prio: enqprio_name_to_val(&group.prio),
                perf: 0, // 0 means don't set the target. Max perf target for schedutils is 1024.
            };
            sm.gmap_insert(&gid, &sg);
        });

        // cache in a struct
        PreAllocatedGroups {
            sm,
            groups: Arc::new(gh),
            cid_map: DashMap::new(),
        }
    }

    pub fn update_cgroup_chrs(&self, gid: i32, ts: u64, dur: u64, arriv: u64, cgroup_id: &str) {
        // update cMap cgroup_id -> gid
        let cval = CgroupChrs {
            gid,
            invoke_ts: ts,
            arrival_ts: arriv,
            workerdur: dur,
        };
        self.cid_map.insert(cgroup_id.to_string(), cval);
        self.sm.cmap_insert(cgroup_id, &cval);
    }

    pub fn update_domain_timeslice(&self, gid: i32, timeslice: u64) {
        let _ = self.sm.gmap_update_timeslice(&gid, timeslice);
    }

    pub fn update_domain_perf_target(&self, gid: i32, perf_target: u32) {
        let _ = self.sm.gmap_update_perf_target(&gid, perf_target);
    }

    pub fn get_domain_scheduler_stats(&self, gid: i32) -> Option<SchedGroupStats> {
        self.sm.gstats_lookup(&gid)
    }

    pub fn get_schedgroup(&self, gid: SchedGroupID) -> Option<SchedGroup> {
        self.groups.get(&gid).map(|v| v.clone())
    }

    pub fn total_groups(&self) -> usize {
        self.groups.len()
    }
}
