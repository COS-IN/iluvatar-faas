use super::structs::{Container, ContainerState};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::{transaction::TransactionId, types::Compute};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::debug;

pub type Subpool = Vec<Container>;
static LEN_ORDERING: Ordering = Ordering::Relaxed;

pub struct ResourcePool {
    /// Containers that are currently not running an invocation
    pub idle_containers: ContainerPool,
    /// Containers that are running an invocation
    pub running_containers: ContainerPool,
}
impl ResourcePool {
    pub fn new(compute: Compute) -> Self {
        ResourcePool {
            idle_containers: ContainerPool::new(format!("idle-{:?}", compute).as_str()),
            running_containers: ContainerPool::new(format!("running-{:?}", compute).as_str()),
        }
    }
}

pub struct ContainerPool {
    pool: DashMap<String, Subpool>,
    len: AtomicU32,
    pool_name: String,
}
impl ContainerPool {
    pub fn new(name: &str) -> Self {
        ContainerPool {
            pool: DashMap::new(),
            len: AtomicU32::new(0),
            pool_name: name.to_string(),
        }
    }

    /// Used to register a new fqdn with the pool
    pub fn register_fqdn(&self, fqdn: String) {
        self.pool.insert(fqdn, Vec::new());
    }

    /// The number of items
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Relaxed)
    }

    /// An iterable of all the containers stored in the pool
    pub fn iter(&self) -> Vec<Container> {
        let mut ret = vec![];
        for subpool in self.pool.iter() {
            for item in (*subpool).iter() {
                ret.push(item.clone());
            }
        }
        ret
    }

    /// Returns a random container for the fqdn, or [None] if none is available
    pub fn get_random_container(&self, fqdn: &String, tid: &TransactionId) -> Option<Container> {
        match self.pool.get_mut(fqdn) {
            Some(mut pool_list) => match (*pool_list).pop() {
                Some(c) => {
                    debug!(tid=%tid, container_id=%c.container_id(), name=%self.pool_name, "Removing random container from pool");
                    self.len.fetch_sub(1, LEN_ORDERING);
                    Some(c)
                }
                None => None,
            },
            None => None,
        }
    }

    /// Returns the best possible state of available containers
    /// If none are available, returns [ContainerState::Cold]
    pub fn has_container(&self, fqdn: &String) -> ContainerState {
        let mut ret = ContainerState::Cold;
        match self.pool.get(fqdn) {
            Some(c) => {
                for cont in &*c {
                    let state = cont.state();
                    if state > ret {
                        ret = state;
                    }
                }
                ret
            }
            None => ret,
        }
    }

    /// Add the container to the pool
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    pub fn add_container(&self, container: Container, tid: &TransactionId) -> Result<()> {
        match self.pool.get_mut(container.fqdn()) {
            Some(mut pool_list) => {
                debug!(tid=%tid, container_id=%container.container_id(), name=%self.pool_name, "Inserting container into pool");
                self.len.fetch_add(1, LEN_ORDERING);
                (*pool_list).push(container);
                Ok(())
            }
            None => anyhow::bail!(
                "Function '{}' was supposed to be readable in pool but could not be found",
                container.fqdn()
            ),
        }
    }

    /// Removes the container if it was found in the pool
    /// Returns [None] if it was not found
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    pub fn remove_container(&self, container: &Container, tid: &TransactionId) -> Option<Container> {
        match self.pool.get_mut(container.fqdn()) {
            Some(mut pool_list) => {
                let pool_list = pool_list.value_mut();
                let (pos, pool_len) = self.find_container_pos(&container, &pool_list);
                if pos < pool_len {
                    debug!(tid=%tid, container_id=%container.container_id(), name=%self.pool_name, "Removing container from pool");
                    self.len.fetch_sub(1, LEN_ORDERING);
                    Some(pool_list.remove(pos))
                } else {
                    None
                }
            }
            None => None,
        }
    }
    fn find_container_pos(&self, container: &Container, pool_list: &Vec<Container>) -> (usize, usize) {
        let pool_len = pool_list.len();
        let mut pos = usize::MAX;
        for (i, iter_cont) in pool_list.iter().enumerate() {
            if container.container_id() == iter_cont.container_id() {
                pos = i;
                break;
            }
        }
        return (pos, pool_len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{containers::simulator::simstructs::SimulatorContainer, registration::RegisteredFunction};
    use iluvatar_library::{types::Isolation, utils::calculate_fqdn};
    use std::sync::Arc;

    #[test]
    fn reg() {
        let cp = ContainerPool::new("test");
        let fqdn = calculate_fqdn(&"name".to_string(), &"vesr".to_string());
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        cp.register_fqdn(fqdn.clone());
        let ctr = Arc::new(SimulatorContainer::new(
            "cid".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        cp.add_container(ctr, &"test".to_string())
            .expect("add should not error");
    }
    #[test]
    fn no_reg_fails() {
        let cp = ContainerPool::new("test");
        let fqdn = calculate_fqdn(&"name".to_string(), &"vesr".to_string());
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        let ctr = Arc::new(SimulatorContainer::new(
            "cid".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        match cp.add_container(ctr, &"test".to_string()) {
            Ok(_) => panic!("Should not get Ok with no registration"),
            Err(_) => (),
        };
    }
    #[test]
    fn get() {
        let tid = "test".to_string();
        let fqdn = calculate_fqdn(&"name".to_string(), &"vesr".to_string());
        let cp = ContainerPool::new("test");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        cp.register_fqdn(fqdn.clone());
        let ctr = Arc::new(SimulatorContainer::new(
            "cid".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        cp.add_container(ctr.clone(), &tid).expect("add should not error");
        let ctr2 = cp.get_random_container(&fqdn, &tid).expect("should return a container");

        assert_eq!(&ctr.container_id, ctr2.container_id(), "Container IDs should match");
    }
    #[test]
    fn remove_returns_correct() {
        let tid = "test".to_string();
        let cp = ContainerPool::new("test");
        let fqdn = calculate_fqdn(&"name".to_string(), &"vesr".to_string());
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        cp.register_fqdn(fqdn.clone());
        let ctr = Arc::new(SimulatorContainer::new(
            "cid".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )) as Container;
        cp.add_container(ctr.clone(), &tid).expect("add should not error");
        let removed = cp.remove_container(&ctr, &tid).expect("should remove a container");
        assert_eq!(ctr.container_id(), removed.container_id(), "Container IDs should match");
    }
    #[test]
    fn remove_means_gone() {
        let tid = "test".to_string();
        let cp = ContainerPool::new("test");
        let fqdn = calculate_fqdn(&"name".to_string(), &"vesr".to_string());
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        cp.register_fqdn(fqdn.clone());
        let ctr = Arc::new(SimulatorContainer::new(
            "cid".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )) as Container;
        cp.add_container(ctr.clone(), &tid).expect("add should not error");
        let removed = cp.remove_container(&ctr, &tid).expect("should remove a container");
        assert_eq!(ctr.container_id(), removed.container_id(), "Container IDs should match");

        match cp.get_random_container(&fqdn, &tid) {
            Some(c) => panic!("No container should be returned, got {}", c.container_id()),
            None => (),
        }
    }
    #[test]
    fn len() {
        let cp = ContainerPool::new("test");
        let fqdn = calculate_fqdn(&"name".to_string(), &"vesr".to_string());
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        let fqdn2 = calculate_fqdn(&"name2".to_string(), &"vesr".to_string());
        let reg2 = Arc::new(RegisteredFunction {
            function_name: "name2".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            memory: 0,
            cpus: 0,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            supported_compute: iluvatar_library::types::Compute::CPU,
            fqdn: "".to_string(),
        });
        cp.register_fqdn(fqdn.clone());
        cp.register_fqdn(fqdn2.clone());
        let ctr = Arc::new(SimulatorContainer::new(
            "cid1".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        cp.add_container(ctr, &"test".to_string())
            .expect("add should not error");
        assert_eq!(cp.len(), 1);
        let ctr = Arc::new(SimulatorContainer::new(
            "cid2".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        cp.add_container(ctr, &"test".to_string())
            .expect("add should not error");
        assert_eq!(cp.len(), 2);
        let ctr = Arc::new(SimulatorContainer::new(
            "cid3".to_string(),
            &fqdn,
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        cp.add_container(ctr, &"test".to_string())
            .expect("add should not error");
        assert_eq!(cp.len(), 3);
        let ctr = Arc::new(SimulatorContainer::new(
            "cid3".to_string(),
            &fqdn2,
            &reg2,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        ));
        cp.add_container(ctr, &"test".to_string())
            .expect("add should not error");
        assert_eq!(cp.len(), 4);

        cp.get_random_container(&fqdn, &"test".to_string())
            .expect("should remove a container");
        assert_eq!(cp.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 11)]
    async fn parallel_len() {
        let cp = Arc::new(ContainerPool::new("test"));
        let mut handles: Vec<tokio::task::JoinHandle<Result<()>>> = vec![];
        let ts: u32 = 15;
        let creates: u32 = 30;
        let barrier = Arc::new(tokio::sync::Barrier::new(ts as usize));
        for t in 0..ts {
            let cp_c = cp.clone();
            let b_c = barrier.clone();
            handles.push(tokio::task::spawn(async move {
                let fqdn = t.to_string();
                let reg = Arc::new(RegisteredFunction {
                    function_name: fqdn.clone(),
                    function_version: "vesr".to_string(),
                    image_name: "img".to_string(),
                    memory: 0,
                    cpus: 0,
                    snapshot_base: "".to_string(),
                    parallel_invokes: 1,
                    isolation_type: Isolation::all(),
                    supported_compute: iluvatar_library::types::Compute::CPU,
                    fqdn: "".to_string(),
                });
                cp_c.register_fqdn(fqdn.clone());
                b_c.wait().await;
                for i in 0..creates {
                    let ctr = Arc::new(SimulatorContainer::new(
                        format!("cid{}", i),
                        &fqdn,
                        &reg,
                        ContainerState::Cold,
                        Isolation::CONTAINERD,
                        Compute::CPU,
                        None,
                    ));
                    cp_c.add_container(ctr, &"test".to_string())?;
                }
                Ok(())
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(cp.len(), ts * creates);
    }
}
