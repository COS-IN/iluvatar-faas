use super::structs::{Container, ContainerState};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::{bail_error, transaction::TransactionId, types::Compute};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::debug;

pub type Subpool = Vec<Container>;
type Pool = DashMap<String, Subpool>;
static LEN_ORDERING: Ordering = Ordering::Relaxed;
#[derive(Debug)]
enum PoolType {
    Idle,
    Running,
}

/// List of containers for each function (fqdn).
pub struct ContainerPool {
    idle_pool: Pool,
    running_pool: Pool,
    /// fqdn->Vec<Container>
    len: AtomicU32,
    pool_name: String,
}
impl ContainerPool {
    pub fn new(compute: Compute) -> Self {
        ContainerPool {
            idle_pool: DashMap::new(),
            running_pool: DashMap::new(),
            len: AtomicU32::new(0),
            pool_name: format!("{:?}", compute),
        }
    }

    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// The number of items
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Relaxed)
    }

    /// An iterable of all the containers stored in the pool
    pub fn iter(&self) -> Vec<Container> {
        let mut ret = vec![];
        for subpool in self.idle_pool.iter() {
            for item in (*subpool).iter() {
                ret.push(item.clone());
            }
        }
        for subpool in self.running_pool.iter() {
            for item in (*subpool).iter() {
                ret.push(item.clone());
            }
        }
        ret
    }

    /// An iterable of all the idle containers stored in the pool
    pub fn iter_idle(&self) -> Vec<Container> {
        let mut ret = vec![];
        for subpool in self.idle_pool.iter() {
            for item in (*subpool).iter() {
                ret.push(item.clone());
            }
        }
        ret
    }

    /// Apply a function to all the idle containers of the given function
    pub fn iter_idle_fqdn(&self, fqdn: &str) -> Vec<Container> {
        let mut ctrs = vec![];
        if let Some(f) = self.idle_pool.get(fqdn) {
            for i in (*f).iter() {
                ctrs.push(i.clone());
            }
        }
        ctrs
    }

    /// Add the container to the pool
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container, pool, pool_type), fields(tid=tid)))]
    fn add_container(&self, container: Container, pool: &Pool, tid: &TransactionId, pool_type: PoolType) {
        debug!(tid=tid, container_id=%container.container_id(), name=%self.pool_name, pool_type=?pool_type, "Inserting container into pool");
        match pool.get_mut(container.fqdn()) {
            Some(mut pool_list) => (*pool_list).push(container),
            None => {
                pool.insert(container.fqdn().clone(), vec![container]);
            },
        }
    }

    /// Returns a random (idle) container for the fqdn, or [None] if none is available
    /// Moves that container to the idle pool
    pub fn activate_random_container(&self, fqdn: &str, tid: &TransactionId) -> Option<Container> {
        match self.idle_pool.get_mut(fqdn) {
            Some(mut pool_list) => match (*pool_list).pop() {
                Some(c) => {
                    debug!(tid=tid, container_id=%c.container_id(), name=%self.pool_name, pool_type=?PoolType::Idle, "Removing random container from pool");
                    self.add_container(c.clone(), &self.running_pool, tid, PoolType::Running);
                    Some(c)
                },
                None => None,
            },
            None => None,
        }
    }

    /// Move the container from the running pool to idle
    pub fn move_to_idle(&self, container: &Container, tid: &TransactionId) -> Result<()> {
        match self.remove_container_pool(container, &self.running_pool, tid, PoolType::Running) {
            Some(c) => {
                self.add_container(c, &self.idle_pool, tid, PoolType::Idle);
                Ok(())
            },
            None => {
                bail_error!(tid=tid, container_id=%container.container_id(), "Supposedly running container was not found in running pool")
            },
        }
    }

    /// Returns true if an idle container with the attached GPU is available
    pub fn has_gpu_container(&self, fqdn: &str, gpu_token: &crate::services::resources::gpu::GpuToken) -> bool {
        match self.idle_pool.get(fqdn) {
            Some(c) => {
                for cont in &*c {
                    if let Some(gpu) = cont.device_resource().as_ref() {
                        if gpu.gpu_hardware_id == gpu_token.gpu_id && cont.state() == ContainerState::Warm {
                            return true;
                        }
                    }
                }
                false
            },
            None => false,
        }
    }

    /// Returns the best possible state of idle containers
    /// If none are available, returns [ContainerState::Cold]
    pub fn has_idle_container(&self, fqdn: &str) -> ContainerState {
        self.best_container_state(fqdn, &self.idle_pool)
    }
    /// Returns the best possible state of all containers
    /// If none are available, returns [ContainerState::Cold]
    pub fn has_container(&self, fqdn: &str) -> ContainerState {
        std::cmp::max(
            self.best_container_state(fqdn, &self.idle_pool),
            self.best_container_state(fqdn, &self.running_pool),
        )
    }

    /// Returns the best possible state of available containers
    /// If none are available, returns [ContainerState::Cold]
    fn best_container_state(&self, fqdn: &str, pool: &Pool) -> ContainerState {
        let mut ret = ContainerState::Cold;
        match pool.get(fqdn) {
            Some(c) => {
                for cont in &*c {
                    ret = std::cmp::max(ret, cont.state());
                }
                ret
            },
            None => ret,
        }
    }

    /// Add the container to the idle pool
    /// If an error occurs, the container will not be placed in the pool
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container), fields(tid=tid)))]
    pub fn add_idle_container(&self, container: Container, tid: &TransactionId) {
        self.len.fetch_add(1, LEN_ORDERING);
        self.add_container(container, &self.idle_pool, tid, PoolType::Idle)
    }

    /// Add the container to the running pool
    /// If an error occurs, the container will not be placed in the pool
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container), fields(tid=tid)))]
    pub fn add_running_container(&self, container: Container, tid: &TransactionId) {
        self.len.fetch_add(1, LEN_ORDERING);
        self.add_container(container, &self.running_pool, tid, PoolType::Running)
    }

    /// Removes the container if it was found in the _idle_ pool.
    /// Returns [None] if it was not found.
    /// Removing a running container can cause instability.
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container), fields(tid=tid)))]
    pub fn remove_container(&self, container: &Container, tid: &TransactionId) -> Option<Container> {
        if let Some(c) = self.remove_container_pool(container, &self.idle_pool, tid, PoolType::Idle) {
            self.len.fetch_sub(1, LEN_ORDERING);
            return Some(c);
        }
        // if let Some(c) = self.remove_container_pool(container, &self.running_pool, tid, PoolType::Running) {
        //     self.len.fetch_sub(1, LEN_ORDERING);
        //     return Some(c);
        // }
        None
    }

    /// Removes the container if it was found in the pool
    /// Returns [None] if it was not found
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, container, pool_type, pool), fields(tid=tid)))]
    fn remove_container_pool(
        &self,
        container: &Container,
        pool: &Pool,
        tid: &TransactionId,
        pool_type: PoolType,
    ) -> Option<Container> {
        match pool.get_mut(container.fqdn()) {
            Some(mut pool_list) => {
                let pool_list = pool_list.value_mut();
                let (pos, pool_len) = self.find_container_pos(container, pool_list);
                if pos < pool_len {
                    debug!(tid=tid, container_id=%container.container_id(), name=%self.pool_name, pool_type=?pool_type, "Removing container from pool");
                    Some(pool_list.remove(pos))
                } else {
                    None
                }
            },
            None => None,
        }
    }
    fn find_container_pos(&self, container: &Container, pool_list: &Subpool) -> (usize, usize) {
        let pool_len = pool_list.len();
        let mut pos = usize::MAX;
        for (i, iter_cont) in pool_list.iter().enumerate() {
            if container.container_id() == iter_cont.container_id() {
                pos = i;
                break;
            }
        }
        (pos, pool_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::{containers::simulator::simstructs::SimulatorContainer, registration::RegisteredFunction};
    use iluvatar_library::transaction::gen_tid;
    use iluvatar_library::{types::Isolation, utils::calculate_fqdn};
    use std::sync::Arc;

    #[test]
    fn reg() {
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        );
        cp.add_idle_container(ctr, &"test".to_string());
    }

    #[test]
    fn get() {
        let tid = "test".to_string();
        let fqdn = calculate_fqdn("name", "vesr");
        let cp = ContainerPool::new(Compute::CPU);
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        );
        cp.add_idle_container(ctr.clone(), &tid);
        let ctr2 = cp
            .activate_random_container(&fqdn, &tid)
            .expect("should return a container");

        assert_eq!(&ctr.container_id, ctr2.container_id(), "Container IDs should match");
    }
    #[test]
    fn remove_returns_correct() {
        let tid = "test".to_string();
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_idle_container(ctr.clone(), &tid);
        let removed = cp.remove_container(&ctr, &tid).expect("should remove a container");
        assert_eq!(ctr.container_id(), removed.container_id(), "Container IDs should match");
    }
    #[test]
    fn remove_means_gone() {
        let tid = "test".to_string();
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_idle_container(ctr.clone(), &tid);
        let removed = cp.remove_container(&ctr, &tid).expect("should remove a container");
        assert_eq!(ctr.container_id(), removed.container_id(), "Container IDs should match");

        match cp.activate_random_container(&fqdn, &tid) {
            Some(c) => panic!("No container should be returned, got {}", c.container_id()),
            None => (),
        }
    }
    #[test]
    fn len() {
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let fqdn2 = calculate_fqdn("name2", "vesr");
        let reg2 = Arc::new(RegisteredFunction {
            function_name: "name2".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid1".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        );
        cp.add_idle_container(ctr, &"test".to_string());
        assert_eq!(cp.len(), 1);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid2".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        );
        cp.add_idle_container(ctr, &"test".to_string());
        assert_eq!(cp.len(), 2);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid3".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        );
        cp.add_idle_container(ctr, &"test".to_string());
        assert_eq!(cp.len(), 3);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid3".to_string(),
                &fqdn2,
                &reg2,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        );
        cp.add_idle_container(ctr, &"test".to_string());
        assert_eq!(cp.len(), 4);

        let c = cp
            .activate_random_container(&fqdn, &"test".to_string())
            .expect("should remove a container");
        assert_eq!(cp.len(), 4);
        cp.move_to_idle(&c, &"test".to_string()).unwrap();
        assert_eq!(cp.len(), 4);
        cp.remove_container(&c, &"test".to_string())
            .expect("should remove a container");
        assert_eq!(cp.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 11)]
    async fn parallel_len() {
        let cp = Arc::new(ContainerPool::new(Compute::CPU));
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
                    parallel_invokes: 1,
                    isolation_type: Isolation::all(),
                    ..std::default::Default::default()
                });
                b_c.wait().await;
                for i in 0..creates {
                    let ctr = Arc::new(
                        SimulatorContainer::new(
                            &gen_tid(),
                            format!("cid{}", i),
                            &fqdn,
                            &reg,
                            ContainerState::Cold,
                            Isolation::CONTAINERD,
                            Compute::CPU,
                            None,
                        )
                        .unwrap(),
                    );
                    cp_c.add_idle_container(ctr, &"test".to_string());
                }
                Ok(())
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_eq!(cp.len(), ts * creates);
    }

    #[test]
    fn move_to_idle() {
        let tid = "test".to_string();
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Cold,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_running_container(ctr.clone(), &tid);
        assert_eq!(cp.running_pool.get(&fqdn).unwrap().len(), 1);
        cp.move_to_idle(&ctr, &tid).expect("move_to_idle should succeed");
        assert_eq!(cp.running_pool.get(&fqdn).unwrap().len(), 0);
        assert_eq!(cp.idle_pool.get(&fqdn).unwrap().len(), 1);
    }

    #[test]
    fn has_idle_container() {
        let tid = "test".to_string();
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        assert_eq!(cp.has_idle_container(&fqdn), ContainerState::Cold);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Prewarm,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_idle_container(ctr, &tid);
        assert_eq!(cp.has_idle_container(&fqdn), ContainerState::Prewarm);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Warm,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_idle_container(ctr, &tid);
        assert_eq!(cp.has_idle_container(&fqdn), ContainerState::Warm);
    }

    #[test]
    fn has_container() {
        let tid = "test".to_string();
        let cp = ContainerPool::new(Compute::CPU);
        let fqdn = calculate_fqdn("name", "vesr");
        let reg = Arc::new(RegisteredFunction {
            function_name: "name".to_string(),
            function_version: "vesr".to_string(),
            image_name: "img".to_string(),
            parallel_invokes: 1,
            isolation_type: Isolation::all(),
            ..std::default::Default::default()
        });
        assert_eq!(cp.has_container(&fqdn), ContainerState::Cold);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Prewarm,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_running_container(ctr, &tid);
        assert_eq!(cp.has_container(&fqdn), ContainerState::Prewarm);
        let ctr = Arc::new(
            SimulatorContainer::new(
                &gen_tid(),
                "cid".to_string(),
                &fqdn,
                &reg,
                ContainerState::Warm,
                Isolation::CONTAINERD,
                Compute::CPU,
                None,
            )
            .unwrap(),
        ) as Container;
        cp.add_idle_container(ctr, &tid);
        assert_eq!(cp.has_container(&fqdn), ContainerState::Warm);
    }
}
