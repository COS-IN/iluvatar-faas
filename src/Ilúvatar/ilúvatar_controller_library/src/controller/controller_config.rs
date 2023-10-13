use config::{Config, File};
use iluvatar_library::{influx::InfluxConfig, logging::LoggingConfig, utils::port_utils::Port};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[allow(unused)]
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
#[allow(unused)]
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

pub type ControllerConfig = Arc<Configuration>;

impl Configuration {
    pub fn new(config_fpath: &String) -> anyhow::Result<Self> {
        let sources = vec![
            "load_balancer/src/load_balancer.json",
            config_fpath.as_str(),
            "load_balancer/src/load_balancer.dev.json",
        ];
        let s = Config::builder()
            .add_source(
                sources
                    .iter()
                    .filter(|path| std::path::Path::new(&path).exists())
                    .map(|path| File::with_name(path))
                    .collect::<Vec<_>>(),
            )
            .add_source(
                config::Environment::with_prefix("ILUVATAR_CONTROLLER")
                    .try_parsing(true)
                    .separator("__"),
            );
        match s.build() {
            Ok(s) => match s.try_deserialize() {
                Ok(cfg) => Ok(cfg),
                Err(e) => anyhow::bail!("Failed to deserialize configuration because '{}'", e),
            },
            Err(e) => anyhow::bail!("Failed to build configuration because '{}'", e),
        }
    }

    pub fn boxed(config_fpath: &String) -> anyhow::Result<ControllerConfig> {
        Ok(Arc::new(Configuration::new(config_fpath)?))
    }
}
