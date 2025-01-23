use std::collections::VecDeque;
use tokio::time::Instant;

#[derive(Debug)]
pub struct DeviceTput {
    tput_record: VecDeque<(Instant, f64)>,
}
impl DeviceTput {
    pub fn new() -> Self {
        DeviceTput {
            tput_record: VecDeque::new(),
        }
    }
    /// Items must be inserted in monotonically increasing with respect to time.
    pub fn insert(&mut self, time: Instant, exec_time: f64) {
        self.tput_record.push_front((time, exec_time));
        if self.tput_record.len() > 20 {
            self.tput_record.pop_back();
        }
    }
    /// Get the throughput / second.
    /// Returns 0 if not enough items are in buffer
    pub fn get_tput(&self) -> f64 {
        if !self.tput_record.is_empty() {
            let tput_sum = self.tput_record.iter().fold(0.0, |acc, i| acc + i.1);
            let elapsed = (self.tput_record.front().unwrap().0 - self.tput_record.back().unwrap().0).as_secs_f64();
            if elapsed != 0.0 {
                return tput_sum / elapsed;
            }
        }
        0.0
    }
}

#[cfg(test)]
mod device_tput {
    use super::*;
    use crate::clock::now;
    use std::ops::Add;
    use std::time::Duration;

    #[test]
    fn items_added() {
        let c = now();
        let mut tracker = DeviceTput::new();
        tracker.insert(c, 0.1);
        tracker.insert(c, 0.1);
        tracker.insert(c, 0.1);
        assert_eq!(tracker.tput_record.len(), 3);
    }

    #[test]
    fn max_buff_size() {
        let c = now();
        let mut tracker = DeviceTput::new();
        for _ in 0..40 {
            tracker.insert(c, 0.1);
        }
        assert_eq!(tracker.tput_record.len(), 20);
    }

    #[test]
    fn tput() {
        let c = now();
        let mut tracker = DeviceTput::new();
        for _ in 0..5 {
            tracker.insert(c, 1.0);
        }
        let c2 = c.add(Duration::from_secs_f64(5.0));
        for _ in 0..5 {
            tracker.insert(c2, 1.0);
        }
        assert_eq!(tracker.get_tput(), 2.0);
    }
}
