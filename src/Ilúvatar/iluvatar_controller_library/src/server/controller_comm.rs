use super::{controller::Controller, rpc::RpcControllerAPI};
use crate::services::ControllerAPI;
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::utils::is_simulation;
use iluvatar_library::{bail_error, transaction::TransactionId, utils::port::Port};
use std::sync::Arc;

pub struct ControllerAPIFactory {
    /// cache of RPC connections to controllers
    /// We can clone them for faster connection
    /// better than opening a new one
    rpc_apis: DashMap<String, RpcControllerAPI>,
    sim_apis: DashMap<String, Arc<Controller>>,
    is_simulation: bool,
}

impl ControllerAPIFactory {
    pub fn boxed() -> Arc<ControllerAPIFactory> {
        Arc::new(ControllerAPIFactory {
            rpc_apis: DashMap::new(),
            sim_apis: DashMap::new(),
            is_simulation: is_simulation(),
        })
    }
}

impl ControllerAPIFactory {
    fn try_get_rpcapi(&self, controller: &str) -> Option<RpcControllerAPI> {
        self.rpc_apis.get(controller).map(|r| r.to_owned())
    }

    fn try_get_simapi(&self, controller: &str) -> Option<Arc<Controller>> {
        self.sim_apis.get(controller).map(|r| r.to_owned())
    }

    /// Get the controller API that matches it's implemented communication method
    pub async fn get_controller_api(&self, host: &str, port: Port, tid: &TransactionId) -> Result<ControllerAPI> {
        match self.is_simulation {
            false => match self.try_get_rpcapi(host) {
                Some(r) => Ok(Arc::new(r) as ControllerAPI),
                None => {
                    let api = match RpcControllerAPI::new(host, port, tid).await {
                        Ok(api) => api,
                        Err(e) => {
                            bail_error!(tid=tid, host=%host, host=%e, "Unable to create API for controller")
                        },
                    };
                    self.rpc_apis.insert(host.to_owned(), api.clone());
                    Ok(Arc::new(api) as ControllerAPI)
                },
            },
            true => {
                let api = match self.try_get_simapi(host) {
                    Some(api) => api,
                    None => match self.sim_apis.entry(host.to_owned()) {
                        dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
                        dashmap::mapref::entry::Entry::Vacant(vacant) => {
                            let controller_config = match super::controller_config::Configuration::boxed(host) {
                                Ok(w) => w,
                                Err(e) => anyhow::bail!("Failed to load config because '{:?}'", e),
                            };
                            let api = Controller::new(controller_config, tid).await?;
                            let api = Arc::new(api);
                            vacant.insert(api.clone());
                            api
                        },
                    },
                };
                Ok(api as ControllerAPI)
            },
        }
    }
}
