#![allow(dead_code)]

// Wants
// 1) Can be disabled
// 2) Clients can send arbitrary data (a single struct) to be kept in buffer
//      Tracked items are updated/computed at regular interval
//      These will keep latest X items from that client with timestamps
//          Or keep X minutes worth of item (keep Y items w.r.t. interval)
//      Values will be logged at some provided level
// 3) Clients can request data
//      External or internal clients (ABI or network)
//      Minimal serialization - make provider client provide transmission setup for struct?
//      Request a range of items, latest or X 'minutes' back
//      Return what?
//          Vec of cloned items
//          Iterator over reference pointers

use crate::clock::now;
use crate::types::ToAny;
use parking_lot::RwLock;
use rcu_cell::RcuCell;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

// punt networking issue
pub trait Wireable {}
pub trait Bufferable: Wireable + ToAny + Send + Sync {}
impl<T: Wireable + ToAny + Send + Sync> Bufferable for T {}

impl Wireable for () {}
impl ToAny for () {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A pointer to a single item in a buffer.
pub type BufferItem = Arc<(Instant, Arc<dyn Bufferable>)>;

/// Buffer storage for a single item.
/// Assumes only one writer will exist.
///     If this is NOT the case, items may be out of order
pub struct BufferVec {
    data: Box<[Arc<RcuCell<(Instant, Arc<dyn Bufferable>)>>]>,
    /// Points to the bucket that will be inserted into *next* (future insertion)
    next_index: RwLock<usize>,
}
impl BufferVec {
    pub fn new(entries: usize) -> Self {
        Self {
            data: {
                let mut v = Vec::with_capacity(entries);
                // This is how RcuCell is intended to work
                #[allow(clippy::arc_with_non_send_sync)]
                (0..entries).for_each(|_| v.push(Arc::new(RcuCell::none())));
                v.into_boxed_slice()
            },
            next_index: RwLock::new(0),
        }
    }

    #[inline(always)]
    /// The most recent entry an item was inserted into.
    fn latest_idx(&self) -> usize {
        match *self.next_index.read() {
            0 => self.data.len() - 1,
            idx => idx - 1,
        }
    }

    pub fn insert(&self, entry: Arc<dyn Bufferable>) {
        let mut idx = self.next_index.write();
        #[allow(clippy::arc_with_non_send_sync)]
        self.data[*idx].write(Arc::new((now(), entry)));
        *idx = (*idx + 1) % self.data.len();
    }

    /// Gets the most recent item, if there is one
    pub fn latest(&self) -> Option<BufferItem> {
        let idx = self.latest_idx();
        self.data[idx].read()
    }

    /// All the entries in the past `previous` timeframe, in descending order.
    /// I.e. the most recent item is first, oldest item is last.
    pub fn history(&self, previous: Duration) -> Vec<BufferItem> {
        let mut v = vec![];
        let mut idx = self.latest_idx();
        while let Some(item) = self.data[idx].read() {
            if item.0.elapsed() > previous {
                break;
            }
            v.push(item.clone());
            idx = match idx {
                0 => self.data.len() - 1,
                idx => idx - 1,
            }
        }
        v
    }
}

pub struct RingBuffer {
    entries: RwLock<HashMap<String, BufferVec>>,
    default_time_keep: Duration,
}
impl RingBuffer {
    pub fn new(default_time_keep: Duration) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            default_time_keep,
        }
    }

    pub fn prepare_entry(&self, key: &str, insert_freq: Duration) -> anyhow::Result<()> {
        let entries = self.default_time_keep.as_secs_f64() * (1.0 / insert_freq.as_secs_f64());
        let buff = BufferVec::new(f64::ceil(entries) as usize);
        let mut lck = self.entries.write();
        if lck.contains_key(key) {
            anyhow::bail!("Key {} already exists in RingBuffer", key);
        }
        lck.insert(key.to_owned(), buff);
        Ok(())
    }

    pub fn insert(&self, key: &str, item: Arc<dyn Bufferable>) {
        let r_lck = self.entries.read();
        if let Some(e) = r_lck.get(key) {
            e.insert(item);
        } else {
            drop(r_lck);
            let mut lck = self.entries.write();
            if let Some(e) = lck.get(key) {
                e.insert(item);
            } else {
                // arbitrary backup value, clients should have set this up when registering callbacks
                let buff = BufferVec::new(30);
                buff.insert(item);
                lck.insert(key.to_owned(), buff);
            }
        }
    }

    /// Gets the most recent item, if there is one
    pub fn latest(&self, key: &str) -> Option<BufferItem> {
        self.entries.read().get(key)?.latest()
    }

    /// All the entries in the past `previous` timeframe, in descending order.
    /// I.e. the most recent item is first, oldest item is last.
    /// Empty if nothing matching found.
    pub fn history(&self, key: &str, previous: Duration) -> Vec<BufferItem> {
        match self.entries.read().get(key) {
            None => vec![],
            Some(e) => e.history(previous),
        }
    }
}

#[cfg(test)]
mod buff_vec_tests {
    use super::*;
    use crate::ToAny;
    use std::thread::sleep;

    #[derive(ToAny)]
    struct Item {
        idx: usize,
    }
    impl Wireable for Item {}

    #[test]
    fn build() {
        let _ = BufferVec::new(10);
    }
    #[test]
    fn insert() {
        let b = BufferVec::new(10);
        for idx in 0..20 {
            b.insert(Arc::new(Item { idx }));
        }
    }
    #[test]
    fn latest() {
        let b = BufferVec::new(10);
        for idx in 0..20 {
            b.insert(Arc::new(Item { idx }));
            let item = b.latest();
            assert!(item.is_some());
            let item = item.unwrap();
            if let Some(i) = crate::downcast!(item.1, Item) {
                assert_eq!(i.idx, idx)
            }
            if let Some(i) = item.1.as_any().downcast_ref::<Item>() {
                assert_eq!(i.idx, idx)
            }
        }
    }
    #[test]
    fn history() {
        let b = BufferVec::new(10);
        for idx in 0..20 {
            b.insert(Arc::new(Item { idx }));
            sleep(Duration::from_millis(10));
        }
        let hist = b.history(Duration::from_millis(51));
        for i in 0..hist.len() {
            if i == hist.len() - 1 {
                break;
            }
            assert!(hist[i].0 > hist[i + 1].0, "History was not in descending order");
        }
        assert!(
            hist.len() == 5 || hist.len() == 4,
            "History length was not an expected size, was: {}",
            hist.len()
        );
    }
}

#[cfg(test)]
mod ring_buff_tests {
    use super::*;
    use crate::threading::tokio_logging_thread;
    use crate::transaction::gen_tid;
    use crate::ToAny;
    use iluvatar_library::transaction::TransactionId;
    use tokio::task::JoinHandle;

    const KEY: &str = "KEY";

    #[derive(ToAny)]
    struct Item {
        idx: usize,
    }
    impl Wireable for Item {}

    #[test]
    fn build() {
        let _ = RingBuffer::new(Duration::from_secs(60));
    }

    #[rstest::rstest]
    #[case(100, 60)]
    #[case(200, 60)]
    #[case(500, 60)]
    #[case(1000, 60)]
    #[case(2000, 60)]
    #[case(100, 30)]
    #[case(200, 30)]
    #[case(500, 30)]
    #[case(1000, 30)]
    #[case(2000, 30)]
    fn compute_entries(#[case] keep_time: u64, #[case] freq: u64) {
        let ring = RingBuffer::new(Duration::from_secs(keep_time));
        assert!(ring.latest(KEY).is_none());
        ring.prepare_entry(KEY, Duration::from_millis(freq)).unwrap();
        assert!(ring.latest(KEY).is_none());
        let entries = ring
            .entries
            .read()
            .get(KEY)
            .expect("Entry should have been made")
            .data
            .len();
        assert_eq!(entries, f64::ceil(keep_time as f64 * (1000.0 / freq as f64)) as usize);
    }

    #[test]
    fn empty() {
        let ring = RingBuffer::new(Duration::from_secs(60));
        assert!(ring.latest(KEY).is_none());
        assert_eq!(ring.history(KEY, Duration::from_secs(60)).len(), 0);
    }

    #[test]
    fn insert() {
        let ring = RingBuffer::new(Duration::from_secs(60));
        ring.insert(KEY, Arc::new(Item { idx: 0 }));
        assert!(ring.latest(KEY).is_some());
        assert_eq!(ring.history(KEY, Duration::from_secs(60)).len(), 1);
    }

    fn reader(last_val: usize, buff: Arc<RingBuffer>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut last = 0;
            loop {
                let item = buff.latest(KEY);
                assert!(item.is_some());
                let item = item.unwrap();
                if let Some(i) = crate::downcast!(item.1, Item) {
                    assert!(i.idx >= last);
                    last = i.idx;
                }
                if last == last_val {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_write() {
        let max = 20;
        let ring = Arc::new(RingBuffer::new(Duration::from_secs(60)));
        let r2 = ring.clone();
        ring.insert(KEY, Arc::new(Item { idx: 0 }));
        let writer = tokio::spawn(async move {
            for idx in 1..max {
                ring.insert(KEY, Arc::new(Item { idx }));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        let reader = reader(max - 1, r2);
        writer.await.unwrap();
        reader.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn readers_writer() {
        let max = 20;
        let ring = Arc::new(RingBuffer::new(Duration::from_secs(60)));
        ring.insert(KEY, Arc::new(Item { idx: 0 }));
        let mut readers = vec![];
        for _ in 0..3 {
            let r2 = ring.clone();
            readers.push(reader(max - 1, r2));
        }

        let writer = tokio::spawn(async move {
            for idx in 1..max {
                ring.insert(KEY, Arc::new(Item { idx }));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        writer.await.unwrap();
        for r in readers {
            r.await.unwrap();
        }
    }

    struct Logger {}
    impl Logger {
        pub async fn call(self: &Arc<Self>, _tid: &TransactionId) -> anyhow::Result<Item> {
            Ok(Item { idx: 0xBADCAFE })
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    /// And End-to-end test of how callees will use RingBuffer, from a periodically called Tokio thread.
    async fn background_writer() {
        let ring = Arc::new(RingBuffer::new(Duration::from_secs(60)));
        let writer = Arc::new(Logger {});
        let key = gen_tid();
        let (_hand, t) = tokio_logging_thread(20, key.clone(), ring.clone(), Logger::call).unwrap();
        t.send(writer).expect("send should work");
        let start = now();
        loop {
            if start.elapsed() > Duration::from_secs(10) {
                panic!("failed to find item after 10 seconds");
            }
            let item = ring.latest(&key);
            if let Some(i) = item {
                if let Some(i) = crate::downcast!(i.1, Item) {
                    assert_eq!(i.idx, 0xBADCAFE);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
