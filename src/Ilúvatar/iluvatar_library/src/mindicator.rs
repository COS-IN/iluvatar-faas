use std::sync::Arc;
use parking_lot::RwLock;

pub struct Mindicator {
    data: RwLock<Vec<u32>>,
}
impl Mindicator {
  /// Returns a new [Mindicator] with [num_procs] entries, zero based
  pub fn boxed(num_procs: usize) -> Arc<Self> {
    let data = vec![u32::MAX; num_procs];
    Arc::new(Mindicator {
      data: RwLock::new(data)
    })
  }

  pub fn insert(&self, proc_id: usize, val: u32) {
      self.data.write()[proc_id as usize] = val;
  }

  pub fn min(&self) -> u32 {
      *self.data.read().iter().min().unwrap_or(&0)
  }

  pub fn remove(&self, proc_id: usize) {
    self.data.write()[proc_id as usize] = u32::MAX;
  }
}

#[cfg(test)]
mod mindicator_tests {
    use std::time::Duration;
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(132)]
    #[case(10)]
    #[case(500)]
    #[case(1)]
    fn data_created_properly(#[case] size: usize) {
        let m = Mindicator::boxed(size);
        assert_eq!(m.data.read().len(), size);
        for val in m.data.read().iter() {
            assert_eq!(val, &u32::MAX);
        }
    }

    #[rstest]
    #[case(132)]
    fn default_min_is_max(#[case] size: usize) {
      let m = Mindicator::boxed(size);
      assert_eq!(m.min(), u32::MAX);
  }

  #[rstest]
  #[case(132)]
  #[case(10)]
  #[case(50)]
  fn inserting_new_min_matched(#[case] size: usize) {
    let m = Mindicator::boxed(size);
    for i in (0..size).rev() {
        m.insert(i, i as u32);
        assert_eq!(m.min(), i as u32);
        assert_eq!(m.data.read()[i], i as u32);
      }
  }

  #[tokio::test(flavor = "multi_thread", worker_threads=10)]
  async fn parallel_inserts_safe() {
    let size = 50;
    let m = Mindicator::boxed(size);
    for i in 0..size {
      let m_c = m.clone();
      tokio::spawn(async move {
          m_c.insert(i, i as u32);
      });
    }
    assert_eq!(m.min(), 0);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads=10)]
  async fn parallel_insert_remove_safe() {
    let size = 50;
    let m = Mindicator::boxed(size);
    let mut ts = vec![];
    for i in 0..size {
      let m_c = m.clone();
      ts.push(tokio::spawn(async move {
          m_c.insert(i, i as u32);
          tokio::time::sleep(Duration::from_millis(2)).await;
          m_c.insert(i, (i as u32)*2);
          tokio::time::sleep(Duration::from_millis(2)).await;
          if i < 10 {
            m_c.remove(i);
          }
      }));
    }
    for t in ts {
      t.await.unwrap();
    }
    assert_eq!(m.min(), 20);
  }

  #[rstest]
  #[case(132)]
  #[case(10)]
  #[case(50)]
  fn removal_updates_min(#[case] size: usize) {
    let m = Mindicator::boxed(size);
    for i in (0..size).rev() {
        m.insert(i, i as u32);
        assert_eq!(m.min(), i as u32);
    }
    for i in 0..size {
      m.remove(i);
      if i == size-1 {
        assert_eq!(m.min(), u32::MAX);
      } else {
        assert_eq!(m.min(), (i as u32)+1);
      }
    }
  }
}
