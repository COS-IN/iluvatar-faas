pub mod rpc;
pub mod services;
pub mod worker_api;
use dashmap::DashMap;
use std::sync::RwLock;

pub static mut SCHED_CHANNELS: Option<RwLock<worker_api::Channels>> = None;
pub static mut FQDN_PID_MAP: Option<RwLock<DashMap<String,u32>>> = None;

pub fn insert_to_fqdn_pid_map( fqdn: String, pid: u32 ){
    unsafe {
        if let Some(fpmap) = &FQDN_PID_MAP {
            let mut fpmap = fpmap.write().unwrap();
            fpmap.insert(fqdn, pid);
        }else{
            FQDN_PID_MAP = Some( RwLock::new( DashMap::new() ) );
        }
    }
}

pub fn get_pid_from_fqdn( fqdn: String ) -> Option<u32> {
    unsafe {
        if let Some(fpmap) = &FQDN_PID_MAP {
            let dmap = fpmap.read().unwrap();
            let pid = dmap.get(&fqdn)?;
            return Some(pid.clone());
        }
    }
    None 
}


