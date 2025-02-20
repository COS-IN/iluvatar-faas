use iluvatar_library::{influx::InfluxConfig, logging::LoggingConfig, utils::port_utils::Port};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct Configuration {
    /// name for the server
    pub name: String,
    /// address to listen on
    pub address: String,
    /// port to listen on
    pub port: Port,
    /// request timeout length in seconds
    pub timeout_sec: u64,
    pub logging: Arc<LoggingConfig>,
    pub load_balancer: Arc<LoadBalancingConfig>,
    pub influx: Arc<InfluxConfig>,
}

#[derive(Debug, Deserialize)]
/// configuration for the load balancer
pub struct LoadBalancingConfig {
    /// the load balancing algorithm to use
    pub algorithm: String,
    /// the load metric to use
    ///   only relevant to those algorithms that use it
    pub load_metric: String,
    /// Duration in milliseconds the balancer's worker thread will sleep between runs (if it has one)
    pub thread_sleep_ms: u64,
}

pub const CONTROLLER_ENV_PREFIX: &str = "ILUVATAR_CONTROLLER";
pub type ControllerConfig = Arc<Configuration>;

impl Configuration {
    pub fn boxed(config_fpath: &str) -> anyhow::Result<ControllerConfig> {
        iluvatar_library::config::load_config::<ControllerConfig>(None, Some(config_fpath), None, CONTROLLER_ENV_PREFIX)
    }
}
