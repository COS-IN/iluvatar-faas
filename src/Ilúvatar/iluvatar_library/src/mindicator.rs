use anyhow::Result;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Mindicator {
    data: RwLock<Vec<f64>>,
}
impl Mindicator {
    /// Returns a new [Mindicator] with [num_procs] entries, zero based
    pub fn boxed(num_procs: usize) -> Arc<Self> {
        let data = vec![f64::MAX; num_procs];
        Arc::new(Mindicator {
            data: RwLock::new(data),
        })
    }

    /// Insert a new value into a given proc.
    /// Returns an error if [f64::is_nan] is true
    pub fn insert(&self, proc_id: usize, val: f64) -> Result<()> {
        if val.is_nan() {
            anyhow::bail!("Value passed to mindicator was NaN!!")
        }
        self.data.write()[proc_id] = val;
        Ok(())
    }

    /// Return the minimum value across all items, of [f64::MAX] if all are empty
    pub fn min(&self) -> f64 {
        *self
            .data
            .read()
            .iter()
            .min_by(|x1, x2| x1.partial_cmp(x2).unwrap())
            .unwrap_or(&f64::MAX)
    }

    /// Unset the min value for the position
    pub fn remove(&self, proc_id: usize) {
        self.data.write()[proc_id] = f64::MAX;
    }

    /// Add additional slots to the mindicator
    /// Returns the new size of the mindicator, so the callee has access to the range [(len-num_procs), len)
    pub fn add_procs(&self, num_procs: usize) -> usize {
        let mut data = self.data.write();
        data.extend(vec![f64::MAX; num_procs].iter());
        data.len()
    }
}

#[cfg(test)]
mod mindicator_tests {
    use super::*;
    use rstest::rstest;
    use std::time::Duration;

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
    #[case(132, 50)]
    #[case(10, 80)]
    #[case(500, 7)]
    #[case(1, 19)]
    fn add_procs_works(#[case] size: usize, #[case] extend: usize) {
        let m = Mindicator::boxed(size);
        assert_eq!(m.data.read().len(), size);
        for val in m.data.read().iter() {
            assert_eq!(val, &f64::MAX);
        }
        let len = m.add_procs(extend);
        assert_eq!(len, size + extend);
        assert_eq!(m.data.read().len(), size + extend);
        for val in m.data.read().iter() {
            assert_eq!(val, &f64::MAX);
        }
    }

    #[rstest]
    #[case(132, 50)]
    #[case(10, 80)]
    #[case(500, 7)]
    #[case(1, 19)]
    fn extended_procs_do_min(#[case] size: usize, #[case] extend: usize) {
        let m = Mindicator::boxed(size);
        for i in (0..size).rev() {
            let insert = size + extend + 100;
            m.insert(i, insert as f64).unwrap();
            assert_eq!(m.min(), insert as f64);
            assert_eq!(m.data.read()[i], insert as f64);
        }
        let len = m.add_procs(extend);
        assert_eq!(len, size + extend);
        for i in ((len - extend)..len).rev() {
            m.insert(i, i as f64).unwrap();
            assert_eq!(m.min(), i as f64);
            assert_eq!(m.data.read()[i], i as f64);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn parallel_insert_remove_safe() {
        let size = 50;
        let m = Mindicator::boxed(size);
        let mut ts = vec![];
        for i in 0..size {
            let m_c = m.clone();
            ts.push(tokio::spawn(async move {
                m_c.insert(i, i as f64).unwrap();
                tokio::time::sleep(Duration::from_millis(2)).await;
                m_c.insert(i, (i as f64) * 2.0).unwrap();
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
            if i == size - 1 {
                assert_eq!(m.min(), f64::MAX);
            } else {
                assert_eq!(m.min(), (i as f64) + 1.0);
            }
        }
    }
}
