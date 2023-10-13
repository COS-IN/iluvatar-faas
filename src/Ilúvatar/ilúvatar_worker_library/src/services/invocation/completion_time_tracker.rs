use parking_lot::RwLock;
use time::{Duration, OffsetDateTime};

/// A struct for tracking when the next invocation is expected to complete
/// Useful for estimating waiting time and invocation completion time
/// This struct is thread-safe
pub struct CompletionTimeTracker {
    items: RwLock<Vec<OffsetDateTime>>,
}

impl CompletionTimeTracker {
    pub fn new() -> Self {
        CompletionTimeTracker {
            items: RwLock::new(vec![]),
        }
    }

    /// Add a new time to the struct
    pub fn add_item(&self, completion_time: OffsetDateTime) {
        let mut items = self.items.write();
        let pos = items.binary_search(&completion_time);
        match pos {
            Ok(p) => items.insert(p, completion_time),
            Err(p) => items.insert(p, completion_time),
        };
    }

    /// Remove the item with the time from the tracker
    pub fn remove_item(&self, completion_time: OffsetDateTime) {
        let mut items = self.items.write();
        let pos = items.binary_search(&completion_time);
        match pos {
            Ok(p) => {
                items.remove(p);
            }
            Err(_) => (),
        };
    }

    /// The duration until the next item is to be completed
    pub fn next_avail(&self) -> Duration {
        if let Some(item) = self.items.read().first() {
            let dur = *item - OffsetDateTime::now_utc();
            if dur.is_negative() {
                return Duration::seconds(0);
            }
            return dur;
        } else {
            return Duration::seconds(0);
        }
    }
}

#[cfg(test)]
mod tracker_tests {
    use super::*;
    use more_asserts::{assert_gt, assert_le, assert_lt};
    use rand::Rng;

    #[test]
    fn added_items_ordered() {
        let time = OffsetDateTime::UNIX_EPOCH;
        let tracker = CompletionTimeTracker::new();

        for i in 0..10 {
            tracker.add_item(time + Duration::seconds(i));
        }

        for i in 3..7 {
            tracker.remove_item(time + Duration::seconds(i));
        }

        for i in 4..6 {
            tracker.remove_item(time + Duration::seconds(i));
        }

        let items = tracker.items.read();
        for i in 0..items.len() {
            if i < items.len() - 1 {
                assert_lt!(
                    items[i],
                    items[i + 1],
                    "Items were out of order: {:?}",
                    *tracker.items.read()
                );
            }
        }
    }

    #[test]
    fn random_insertions_ordered() {
        let time = OffsetDateTime::UNIX_EPOCH;
        let tracker = CompletionTimeTracker::new();
        for _ in 0..100 {
            let num = rand::thread_rng().gen_range(0..100);
            tracker.add_item(time + Duration::seconds(num));
        }
        let items = tracker.items.read();
        for i in 0..items.len() {
            if i < items.len() - 1 {
                assert_le!(
                    items[i],
                    items[i + 1],
                    "Items were out of order: {:?}",
                    *tracker.items.read()
                );
            }
        }
    }

    #[test]
    fn next_avail_changes() {
        let time = OffsetDateTime::now_utc() + Duration::seconds(10);
        let tracker = CompletionTimeTracker::new();

        tracker.add_item(time);
        assert_ne!(tracker.next_avail(), tracker.next_avail());
    }

    #[test]
    fn single_item_works() {
        let time = OffsetDateTime::now_utc() + Duration::seconds(10);
        let tracker = CompletionTimeTracker::new();

        tracker.add_item(time);
        let time = tracker.next_avail();
        assert_gt!(time.as_seconds_f64(), 0.0);
        assert_le!(time.as_seconds_f64(), 10.0);
    }

    #[test]
    fn no_item_zero() {
        let tracker = CompletionTimeTracker::new();

        let time = tracker.next_avail();
        assert_eq!(time.as_seconds_f64(), 0.0);
    }

    #[test]
    fn newer_item_changes() {
        let tracker = CompletionTimeTracker::new();

        tracker.add_item(OffsetDateTime::now_utc() + Duration::seconds(10));
        tracker.add_item(OffsetDateTime::now_utc() + Duration::seconds(5));
        let time = tracker.next_avail();
        assert_gt!(time.as_seconds_f64(), 0.0);
        assert_le!(time.as_seconds_f64(), 5.0);
    }

    #[test]
    fn item_removal_changes() {
        let tracker = CompletionTimeTracker::new();

        tracker.add_item(OffsetDateTime::now_utc() + Duration::seconds(10));

        let time2 = OffsetDateTime::now_utc() + Duration::seconds(5);
        tracker.add_item(time2);
        let time = tracker.next_avail();
        assert_gt!(time.as_seconds_f64(), 0.0);
        assert_le!(time.as_seconds_f64(), 5.0);
        println!("removing");
        tracker.remove_item(time2);

        let time = tracker.next_avail();
        assert_gt!(time.as_seconds_f64(), 5.0);
        assert_le!(time.as_seconds_f64(), 10.0);
    }
}
