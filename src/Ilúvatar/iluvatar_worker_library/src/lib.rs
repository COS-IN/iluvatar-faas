pub mod rpc;
pub mod services;
pub mod worker_api;
use dashmap::DashMap;

pub static mut SCHED_CHANNELS: Option<worker_api::Channels> = None;
pub static mut FQDN_PID_MAP: Option<DashMap<String,u32>> = None;

