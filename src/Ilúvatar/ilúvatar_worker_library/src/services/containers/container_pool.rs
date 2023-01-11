use std::sync::atomic::{AtomicU32, Ordering};
use dashmap::DashMap;
use iluvatar_library::transaction::TransactionId;
use tracing::{debug};
use anyhow::Result;
use super::structs::Container;

pub type Subpool = Vec<Container>;

pub struct ContainerPool {
  pool: DashMap<String, Subpool>,
  len: AtomicU32,
  pool_name: String
}
impl ContainerPool {
  pub fn new(name: &str) -> Self {
    ContainerPool{
      pool: DashMap::new(),
      len: AtomicU32::new(0),
      pool_name: name.to_string()
    }
  }

  pub fn register_fqdn(&self, fqdn: String) {
    self.pool.insert(fqdn, Vec::new());
  }

  pub fn len(&self) -> u32 {
    self.len.load(Ordering::Relaxed)
  }

  pub fn iter(&self) -> Vec<Container> {
    let mut ret = vec![];
    for subpool in self.pool.iter() {
      for item in (*subpool).iter() {
        ret.push(item.clone());
      }
    }
    ret
  }

  pub fn get_random_container(&self, fqdn: &String, tid: &TransactionId) -> Option<Container> {
    match self.pool.get_mut(fqdn) {
      Some(mut pool_list) => {
        match (*pool_list).pop() {
          Some(c) => {
            debug!(tid=%tid, container_id=%c.container_id(), name=%self.pool_name, "Removing random container from pool");
            self.len.fetch_sub(1, Ordering::Relaxed);
            Some(c)
          },
          None => None
        }
      },
      None => None,
    }
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn, container), fields(tid=%tid)))]
  pub fn add_container(&self, container: Container, tid: &TransactionId) -> Result<()> {
    match self.pool.get_mut(container.fqdn()) {
      Some(mut pool_list) => {
        // let pool_list = pool_list.value_mut();
        debug!(tid=%tid, container_id=%container.container_id(), name=%self.pool_name, "Inserting container into pool");
        (*pool_list).push(container);
        Ok(())
      },
      None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", container.fqdn()),
    }
  }

  // fn get_ensure_subpool(&self, fqdn: &String) -> Subpool {
  //   match self.pool.get(fqdn) {
  //     Some(p) => (*p).clone(),
  //     None => {
  //       let new = Arc::new(DashMap::new());
  //       self.pool.insert(fqdn.clone(), new.clone());
  //       new
  //     },
  //   }
  // }
  // fn get_mut_ensure_subpool(&self, fqdn: &String) -> Subpool {
  //   match self.pool.get(fqdn) {
  //     Some(p) => (*p).clone(),
  //     None => {
  //       let new = Arc::new(DashMap::new());
  //       self.pool.insert(fqdn.clone(), new.clone());
  //       new
  //     },
  //   }
  // }

  // pub fn contains(&self, container: &Container) -> bool {
  //   match self.pool.get(container.fqdn()) {
  //     Some(sub) => {
  //       for c in (*sub).iter() {
  //         if c.container_id() == container.container_id() {
  //           return true;
  //         }
  //       }
  //       return false
  //     },
  //     None => false,
  //   }      
  // }

  pub fn remove_container(&self, container: &Container, tid: &TransactionId) -> Option<Container> {
    match self.pool.get_mut(container.fqdn()) {
      Some(mut pool_list) => {
        let pool_list = pool_list.value_mut();
        // match pool_list.remove(container.container_id()) {
        //   None => anyhow::bail!("Was unable to find container {} to remove it", container.container_id()),
        //   Some(_) => Ok(()),
        // } 
        let (pos, pool_len) = self.find_container_pos(&container, &pool_list);
        if pos < pool_len {
          debug!(tid=%tid, container_id=%container.container_id(), name=%self.pool_name, "Removing container from pool");
          Some(pool_list.remove(pos))
        } else {
          // anyhow::bail!("Was unable to find container {} to remove it", container.container_id())
          None
        }
      },
      // None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", container.fqdn()),
      None => None
    }
  }
  fn find_container_pos(&self, container: &Container, pool_list: &Vec<Container>) -> (usize,usize) {
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
