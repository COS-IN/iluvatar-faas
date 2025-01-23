use crate::worker_api::worker_config::ContainerResourceConfig;
use iluvatar_library::types::MemSizeMb;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// TODO: integrate this into things to have central memory source
pub struct MemoryResourceTracker {
    free_memory: Arc<Semaphore>,
    config: Arc<ContainerResourceConfig>,
}
impl MemoryResourceTracker {
    pub fn boxed(config: &Arc<ContainerResourceConfig>) -> Arc<Self> {
        Arc::new(MemoryResourceTracker {
            free_memory: Arc::new(Semaphore::new(config.memory_mb as usize)),
            config: config.clone(),
        })
    }

    /// Return an amount of memory
    pub fn return_memory(&self, amount: MemSizeMb) {
        self.free_memory.add_permits(amount as usize)
    }

    /// Reduce the amount of available memory, returns [true] if the request was fulfilled
    pub fn reserve_memory(&self, amount: MemSizeMb) -> bool {
        match self.free_memory.try_acquire_many(amount as u32) {
            Ok(p) => {
                p.forget();
                true
            },
            Err(_) => false,
        }
    }

    /// Get a permit that will return the memory when dropped
    /// [None] if not enough memory available
    pub fn try_acquire_memory(&self, amount: MemSizeMb) -> Option<OwnedSemaphorePermit> {
        self.free_memory.clone().try_acquire_many_owned(amount as u32).ok()
    }

    /// Get the amount of available memory
    pub fn available_memory(&self) -> MemSizeMb {
        self.free_memory.available_permits() as MemSizeMb
    }

    pub fn under_pressure(&self) -> bool {
        self.available_memory() <= self.config.memory_buffer_mb
    }
}

#[cfg(test)]
mod memory_resource_tests {
    use super::*;

    fn tracker(memory: MemSizeMb, buffer: MemSizeMb) -> Arc<MemoryResourceTracker> {
        let cfg = ContainerResourceConfig {
            memory_mb: memory,
            memory_buffer_mb: buffer,
            ..Default::default()
        };
        MemoryResourceTracker::boxed(&Arc::new(cfg))
    }

    #[test]
    fn correct_available_mem() {
        let track = tracker(1024, 512);
        assert_eq!(track.available_memory(), 1024 as MemSizeMb);
        track.reserve_memory(512);
        assert_eq!(track.available_memory(), 512);
    }

    #[test]
    fn checks_under_pressure() {
        let track = tracker(1024, 512);
        assert!(!track.under_pressure(), "No allocated mem is not under pressure");
        track.reserve_memory(512);
        assert!(track.under_pressure(), "Enough allocation should cause pressure");
    }

    #[test]
    fn return_memory() {
        let track = tracker(1024, 512);
        assert!(!track.under_pressure(), "No allocated mem is not under pressure");
        track.reserve_memory(512);
        assert!(track.under_pressure(), "Enough allocation should cause pressure");
    }
}
