pub mod services;
pub mod utils;
pub mod worker_api;
use crate::worker_api::fs_scheduler::Channels;
use std::sync::RwLock;

pub static mut SCHED_CHANNELS: Option<RwLock<Channels>> = None;
