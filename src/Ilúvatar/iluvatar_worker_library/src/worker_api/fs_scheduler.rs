use crate::utils::characteristics_map::CharacteristicsPacket;
use crate::worker_api::worker_config::WorkerConfig;
use iluvatar_bpf_library::bpf::func_characs::*;
use iluvatar_library::utils::execute_cmd_nonblocking;
use ipc_channel::ipc::{IpcOneShotServer, IpcSender};

use crate::SCHED_CHANNELS;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::mpsc::{channel, Sender};
use std::sync::RwLock;
use std::thread;

use tracing::debug;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Channels {
    pub tx_chr: IpcSender<CharacteristicsPacket>,
}

pub fn launch_scheduler(worker_config: &WorkerConfig) {
    match &worker_config.finescheduling {
        Some(fconfig) => {
            let fconfig = fconfig.clone();

            if Path::new(&fconfig.binary).exists() {
                // create a oneshot IPC server
                let (server, name) = IpcOneShotServer::new().unwrap();
                debug!(name=%name, status="server waiting", "fine_scheduler");

                let mut args = Vec::<String>::new();

                // default args for all the policies
                args.push("--server-name".to_string());
                args.push(name.clone());

                // construct args for different policies as needed
                match fconfig.binary.as_str() {
                    "/tmp/iluvatar/bin/fs_policy_constrained" => {
                        if let Some(cores) = fconfig.cores.as_ref() {
                            for c in cores {
                                args.push("-c".to_string());
                                args.push(c.to_string());
                            }
                        }
                    }
                    _ => {}
                }

                // launch the policy in a separate thread
                let bname = fconfig.binary.clone();
                thread::spawn(move || {
                    let mut _child = execute_cmd_nonblocking(&bname, &args, None, &String::from("none")).unwrap();

                    let mut buffer = [0; 1024];
                    let mut log = File::create("/tmp/iluvatar/bin/sched.log").expect("failed to open log");
                    let mut elog = File::create("/tmp/iluvatar/bin/sched.elog").expect("failed to open log");

                    loop {
                        let read = _child.stdout.as_mut().unwrap().read(&mut buffer).unwrap_or(0);
                        if read > 0 {
                            let _ = log.write(&buffer[..read]);
                            let _ = log.flush();
                        }
                        match _child.try_wait() {
                            Ok(Some(_status)) => {
                                let read = _child.stderr.as_mut().unwrap().read(&mut buffer).unwrap_or(0);
                                if read > 0 {
                                    let _ = elog.write(&buffer[..read]);
                                    let _ = elog.flush();
                                }
                            }
                            Ok(None) => {}
                            Err(_e) => {}
                        }
                    }
                });

                // wait for the channel to establish with a timeout
                let (_, channels): (_, Channels) = server.accept().unwrap();
                debug!(name=%name, status="channels established", "fine_scheduler");

                // save it in the global variable
                unsafe {
                    SCHED_CHANNELS = Some(RwLock::new(channels));
                }
            }
        }
        None => (), // no binary config found
    }; // end of config match
}

pub fn create_shared_map() -> Sender<(BPF_FMAP_KEY, CharVal)> {
    // create a multi-producer and single consumer async channel
    let (tx, rx) = channel::<(BPF_FMAP_KEY, CharVal)>();

    // move the consumer end to a separate thread
    // where data is pushed to the map
    thread::spawn(move || {
        // build the bpf characteristics map
        let mut open_object = MaybeUninit::uninit();
        let skel = build_and_load(&mut open_object).unwrap();
        let fcmap = skel.maps.func_metadata;

        // unbounded receiver waiting for all senders to complete.
        while let Ok((key, val)) = rx.recv() {
            update_map(&fcmap, &key, &val);
        }
    });

    tx
}
