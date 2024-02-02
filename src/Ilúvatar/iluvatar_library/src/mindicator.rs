use std::sync::Arc;
use parking_lot::RwLock;
use anyhow::Result;

pub struct Mindicator {
    data: RwLock<Vec<f64>>,
}
impl Mindicator {
  /// Returns a new [Mindicator] with [num_procs] entries, zero based
  pub fn boxed(num_procs: usize) -> Arc<Self> {
    let data = vec![f64::MAX; num_procs];
    Arc::new(Mindicator {
      data: RwLock::new(data)
    })
  }

  pub fn insert(&self, proc_id: usize, val: f64) -> Result<()> {
      if val.is_nan() {
          anyhow::bail!("Value passed to mindicator was NaN!!")
      }
      self.data.write()[proc_id as usize] = val;
      Ok(())
  }

  pub fn min(&self) -> f64 {
      *self.data.read().iter().min_by(|x1, x2| x1.partial_cmp(x2).unwrap()).unwrap_or(&f64::MAX)
  }

  pub fn remove(&self, proc_id: usize) {
    self.data.write()[proc_id as usize] = f64::MAX;
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
            assert_eq!(val, &f64::MAX);
        }
    }

    #[rstest]
    #[case(132)]
    fn default_min_is_max(#[case] size: usize) {
      let m = Mindicator::boxed(size);
      assert_eq!(m.min(), f64::MAX);
    }

    #[rstest]
    #[case(132)]
    fn nan_fails(#[case] size: usize) {
      let m = Mindicator::boxed(size);
      assert!(m.insert(0, f64::NAN).is_err());
    }

  #[rstest]
  #[case(132)]
  #[case(10)]
  #[case(50)]
  fn inserting_new_min_matched(#[case] size: usize) {
    let m = Mindicator::boxed(size);
    for i in (0..size).rev() {
        m.insert(i, i as f64).unwrap();
        assert_eq!(m.min(), i as f64);
        assert_eq!(m.data.read()[i], i as f64);
      }
  }

  #[tokio::test(flavor = "multi_thread", worker_threads=10)]
  async fn parallel_inserts_safe() {
    let size = 50;
    let m = Mindicator::boxed(size);
    for i in 0..size {
      let m_c = m.clone();
      tokio::spawn(async move {
          m_c.insert(i, i as f64).unwrap();
      });
    }
    assert_eq!(m.min(), 0.0);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads=10)]
  async fn parallel_insert_remove_safe() {
    let size = 50;
    let m = Mindicator::boxed(size);
    let mut ts = vec![];
    for i in 0..size {
      let m_c = m.clone();
      ts.push(tokio::spawn(async move {
          m_c.insert(i, i as f64).unwrap();
          tokio::time::sleep(Duration::from_millis(2)).await;
          m_c.insert(i, (i as f64)*2.0).unwrap();
          tokio::time::sleep(Duration::from_millis(2)).await;
          if i < 10 {
            m_c.remove(i);
          }
      }));
    }
    for t in ts {
      t.await.unwrap();
    }
    assert_eq!(m.min(), 20.0);
  }

  #[rstest]
  #[case(132)]
  #[case(10)]
  #[case(50)]
  fn removal_updates_min(#[case] size: usize) {
    let m = Mindicator::boxed(size);
    for i in (0..size).rev() {
        m.insert(i, i as f64).unwrap();
        assert_eq!(m.min(), i as f64);
    }
    for i in 0..size {
      m.remove(i);
      if i == size-1 {
        assert_eq!(m.min(), f64::MAX);
      } else {
        assert_eq!(m.min(), (i as f64)+1.0);
      }
    }
  }
}
