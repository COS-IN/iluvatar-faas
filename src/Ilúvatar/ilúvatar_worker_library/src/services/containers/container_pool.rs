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
    // TODO: a proper iterator for this
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

  /// Returns true if any container is present for the matching fqdn
  pub fn has_container(&self, fqdn: &String) -> bool {
    match self.pool.get(fqdn) {
      Some(c) => (*c).len() > 0,
      None => false,
    }
  }

  /// Add the container to the pool
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn, container), fields(tid=%tid)))]
  pub fn add_container(&self, container: Container, tid: &TransactionId) -> Result<()> {
    match self.pool.get_mut(container.fqdn()) {
      Some(mut pool_list) => {
        debug!(tid=%tid, container_id=%container.container_id(), name=%self.pool_name, "Inserting container into pool");
        self.len.fetch_add(1, Ordering::Relaxed);
        (*pool_list).push(container);
        Ok(())
      },
      None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", container.fqdn()),
    }
  }

  /// Removes the container if it was found in the pool
  /// Returns [None] if it was not found
  pub fn remove_container(&self, container: &Container, tid: &TransactionId) -> Option<Container> {
    match self.pool.get_mut(container.fqdn()) {
      Some(mut pool_list) => {
        let pool_list = pool_list.value_mut();
        let (pos, pool_len) = self.find_container_pos(&container, &pool_list);
        if pos < pool_len {
          debug!(tid=%tid, container_id=%container.container_id(), name=%self.pool_name, "Removing container from pool");
          self.len.fetch_sub(1, Ordering::Relaxed);
          Some(pool_list.remove(pos))
        } else {
          None
        }
      },
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
