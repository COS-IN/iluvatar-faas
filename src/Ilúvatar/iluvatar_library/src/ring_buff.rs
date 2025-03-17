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
use std::sync::Arc;
use tokio::time::Instant;

// punt networking issue
pub trait Wireable {}
pub trait Bufferable: Wireable + ToAny {}
impl<T: Wireable + ToAny> Bufferable for T {}

/// Buffer storage for a single item.
/// Assumes only one writer will exist.
///     If this is NOT the case, items may be out of order
pub struct BufferVec {
    data: Vec<Arc<RcuCell<(Instant, Box<dyn Bufferable>)>>>,
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
                v
            },
            next_index: RwLock::new(0),
        }
    }

    pub fn insert(&self, entry: Box<dyn Bufferable>) {
        let mut idx = self.next_index.write();
        #[allow(clippy::arc_with_non_send_sync)]
        self.data[*idx].write(Arc::new((now(), entry)));
        *idx = (*idx + 1) % self.data.len();
    }

    pub fn latest(&self) -> Option<Arc<(Instant, Box<dyn Bufferable>)>> {
        let mut idx = *self.next_index.read();
        idx = match idx {
            0 => self.data.len() - 1,
            idx => idx - 1,
        };
        self.data[idx].read()
    }
}

// pub struct RingBuffer {
//     entries: RwLock<HashMap<String, BufferVec>>,
// }
// impl RingBuffer {
//     pub fn new() -> Self {
//         Self {
//             entries: RwLock::new(HashMap::new()),
//         }
//     }
//
//     // pub fn insert(&mut self, key: &str, item: &dyn Wireable) {
//     //
//     // }
// }

#[cfg(test)]
mod buff_vec_tests {
    use super::*;
    use crate::ToAny;

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
            b.insert(Box::new(Item { idx }));
        }
    }
    #[test]
    fn latest() {
        let b = BufferVec::new(10);
        for idx in 0..20 {
            b.insert(Box::new(Item { idx }));
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
}
