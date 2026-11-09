use crate::bpf_intf::{
    cpu_topology_flags_CPU_TOPOLOGY_BIG as CPU_TOPOLOGY_BIG,
    cpu_topology_flags_CPU_TOPOLOGY_LITTLE as CPU_TOPOLOGY_LITTLE,
    cpu_topology_flags_CPU_TOPOLOGY_ONLINE as CPU_TOPOLOGY_ONLINE,
    cpu_topology_flags_CPU_TOPOLOGY_POSSIBLE as CPU_TOPOLOGY_POSSIBLE,
    cpu_topology_flags_CPU_TOPOLOGY_SMT as CPU_TOPOLOGY_SMT,
};
use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default)]
pub(crate) struct CpuTopologyEntry {
    pub id: u32,
    pub numa_node: i32,
    pub package_id: i32,
    pub die_id: i32,
    pub cluster_id: i32,
    pub core_id: i32,
    pub capacity: u32,
    pub max_freq_khz: u32,
    pub core_type: u32,
    pub flags: u32,
    pub smt_sibling_count: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct SystemTopology {
    pub cpus: Vec<CpuTopologyEntry>,
    pub nr_cpu_ids: u32,
    pub nr_online_cpus: u32,
    pub last_online_cpu: u32,
}

impl SystemTopology {
    pub fn discover(max_cpu_ids: u32) -> Result<Self> {
        Self::discover_from(Path::new("/sys/devices/system/cpu"), max_cpu_ids)
    }

    fn discover_from(root: &Path, max_cpu_ids: u32) -> Result<Self> {
        let possible = parse_cpu_list(&read_required(root.join("possible"))?)?;
        if possible.is_empty() {
            bail!("the kernel CPU possible list is empty");
        }

        let nr_cpu_ids = possible.iter().copied().max().unwrap() + 1;
        if nr_cpu_ids > max_cpu_ids {
            bail!(
                "kernel CPU ID range 0..{} exceeds FineSched capacity {}",
                nr_cpu_ids - 1,
                max_cpu_ids
            );
        }

        let online_list = read_optional(root.join("online"))
            .map(|value| parse_cpu_list(&value))
            .transpose()?
            .unwrap_or_else(|| possible.clone());
        let online: HashSet<u32> = online_list.into_iter().collect();

        let mut cpus = Vec::with_capacity(possible.len());
        for id in possible {
            let cpu_root = root.join(format!("cpu{id}"));
            let is_online = online.contains(&id);
            let siblings = read_optional(cpu_root.join("topology/thread_siblings_list"))
                .map(|value| parse_cpu_list(&value))
                .transpose()?
                .unwrap_or_else(|| vec![id]);

            let mut flags = CPU_TOPOLOGY_POSSIBLE;
            if is_online {
                flags |= CPU_TOPOLOGY_ONLINE;
            }
            if siblings.len() > 1 {
                flags |= CPU_TOPOLOGY_SMT;
            }

            cpus.push(CpuTopologyEntry {
                id,
                numa_node: discover_numa_node(&cpu_root).unwrap_or(-1),
                package_id: read_i32(cpu_root.join("topology/physical_package_id")).unwrap_or(-1),
                die_id: read_i32(cpu_root.join("topology/die_id")).unwrap_or(-1),
                cluster_id: read_i32(cpu_root.join("topology/cluster_id")).unwrap_or(-1),
                core_id: read_i32(cpu_root.join("topology/core_id")).unwrap_or(-1),
                capacity: read_u32(cpu_root.join("cpu_capacity")).unwrap_or(0),
                max_freq_khz: read_u32(cpu_root.join("cpufreq/cpuinfo_max_freq")).unwrap_or(0),
                core_type: read_u32(cpu_root.join("topology/core_type")).unwrap_or(0),
                flags,
                smt_sibling_count: siblings.len() as u32,
            });
        }

        classify_core_sizes(&mut cpus);

        let online_ids: Vec<u32> = cpus
            .iter()
            .filter(|cpu| cpu.flags & CPU_TOPOLOGY_ONLINE != 0)
            .map(|cpu| cpu.id)
            .collect();
        if online_ids.is_empty() {
            bail!("the kernel CPU online list is empty");
        }

        Ok(Self {
            cpus,
            nr_cpu_ids,
            nr_online_cpus: online_ids.len() as u32,
            last_online_cpu: *online_ids.iter().max().unwrap(),
        })
    }

    pub fn online_cpu_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.cpus
            .iter()
            .filter(|cpu| cpu.flags & CPU_TOPOLOGY_ONLINE != 0)
            .map(|cpu| cpu.id)
    }

    pub fn big_cpu_count(&self) -> usize {
        self.cpus.iter().filter(|cpu| cpu.flags & CPU_TOPOLOGY_BIG != 0).count()
    }

    pub fn little_cpu_count(&self) -> usize {
        self.cpus
            .iter()
            .filter(|cpu| cpu.flags & CPU_TOPOLOGY_LITTLE != 0)
            .count()
    }
}

fn classify_core_sizes(cpus: &mut [CpuTopologyEntry]) {
    // Prefer an architecture-provided core type, then scheduler capacity, then
    // maximum frequency. A homogeneous machine intentionally gets neither flag.
    let score = |cpu: &CpuTopologyEntry, source: usize| match source {
        0 => cpu.core_type,
        1 => cpu.capacity,
        _ => cpu.max_freq_khz,
    };

    for source in 0..3 {
        let values: HashSet<u32> = cpus
            .iter()
            .filter(|cpu| cpu.flags & CPU_TOPOLOGY_ONLINE != 0)
            .map(|cpu| score(cpu, source))
            .filter(|value| *value != 0)
            .collect();
        if values.len() < 2 {
            continue;
        }

        let max_score = *values.iter().max().unwrap();
        for cpu in cpus.iter_mut().filter(|cpu| cpu.flags & CPU_TOPOLOGY_ONLINE != 0) {
            let value = score(cpu, source);
            if value == 0 {
                continue;
            }
            cpu.flags |= if value == max_score {
                CPU_TOPOLOGY_BIG
            } else {
                CPU_TOPOLOGY_LITTLE
            };
        }
        return;
    }
}

fn discover_numa_node(cpu_root: &Path) -> Option<i32> {
    fs::read_dir(cpu_root).ok()?.flatten().find_map(|entry| {
        entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("node"))
            .and_then(|node| node.parse().ok())
    })
}

fn parse_cpu_list(value: &str) -> Result<Vec<u32>> {
    let mut cpus = Vec::new();
    for part in value.trim().split(',').filter(|part| !part.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start: u32 = start.parse().with_context(|| format!("invalid CPU list: {value}"))?;
            let end: u32 = end.parse().with_context(|| format!("invalid CPU list: {value}"))?;
            if start > end {
                bail!("invalid descending CPU range {part}");
            }
            cpus.extend(start..=end);
        } else {
            cpus.push(part.parse().with_context(|| format!("invalid CPU list: {value}"))?);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}

fn read_required(path: PathBuf) -> Result<String> {
    fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))
}

fn read_optional(path: PathBuf) -> Option<String> {
    fs::read_to_string(path).ok()
}

fn read_u32(path: PathBuf) -> Option<u32> {
    let value = read_optional(path)?;
    let value = value.trim();
    if let Some(hex) = value.strip_prefix("0x") {
        u32::from_str_radix(hex, 16).ok()
    } else {
        value.parse().ok()
    }
}

fn read_i32(path: PathBuf) -> Option<i32> {
    read_optional(path)?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sparse_cpu_lists() {
        assert_eq!(parse_cpu_list("0-3,8,10-11\n").unwrap(), vec![0, 1, 2, 3, 8, 10, 11]);
    }

    #[test]
    fn classifies_by_frequency_when_capacity_is_uniform() {
        let mut cpus = vec![
            CpuTopologyEntry {
                id: 0,
                capacity: 1024,
                max_freq_khz: 5_000_000,
                flags: CPU_TOPOLOGY_ONLINE,
                ..Default::default()
            },
            CpuTopologyEntry {
                id: 1,
                capacity: 1024,
                max_freq_khz: 3_000_000,
                flags: CPU_TOPOLOGY_ONLINE,
                ..Default::default()
            },
        ];

        classify_core_sizes(&mut cpus);

        assert_ne!(cpus[0].flags & CPU_TOPOLOGY_BIG, 0);
        assert_ne!(cpus[1].flags & CPU_TOPOLOGY_LITTLE, 0);
    }

    #[test]
    fn leaves_homogeneous_cpus_unclassified() {
        let mut cpus = vec![
            CpuTopologyEntry {
                flags: CPU_TOPOLOGY_ONLINE,
                capacity: 1024,
                max_freq_khz: 3_000_000,
                ..Default::default()
            },
            CpuTopologyEntry {
                flags: CPU_TOPOLOGY_ONLINE,
                capacity: 1024,
                max_freq_khz: 3_000_000,
                ..Default::default()
            },
        ];

        classify_core_sizes(&mut cpus);

        assert_eq!(cpus[0].flags & (CPU_TOPOLOGY_BIG | CPU_TOPOLOGY_LITTLE), 0);
        assert_eq!(cpus[1].flags & (CPU_TOPOLOGY_BIG | CPU_TOPOLOGY_LITTLE), 0);
    }
}
