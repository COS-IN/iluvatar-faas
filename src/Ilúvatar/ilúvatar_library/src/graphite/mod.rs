use std::collections::HashMap;
use crate::utils::port_utils::Port;

pub mod graphite_svc;

#[derive(Debug, serde::Deserialize)]
/// Configuration needed to talk to Graphite metrics server
pub struct GraphiteConfig {
  /// Host name server is on
  pub address: String,
  /// Port API is listening on
  pub api_port: Port,
  /// ingestion port
  pub ingestion_port: Port,
  /// Is it listening on UDP? true: yes
  pub ingestion_udp: bool,
  /// If false, calls to graphite will be a no-op and not report data to Graphite
  pub enabled: bool,
}

// [{"target": "worker.load.loadavg", "tags": {"name": "worker.load.loadavg"}, "datapoints": [[3.81, 1658417660], [4.29, 1658417670]]}]
type GraphiteResponse<T> = Vec<Metric<T>>;

// {"target": "worker.load.loadavg", "tags": {"name": "worker.load.loadavg"}, "datapoints": [[3.81, 1658417660], [4.29, 1658417670]]}
#[derive(Debug, serde::Deserialize)]
#[allow(unused)]
struct Metric<T> {
  pub target: String,
  pub tags: HashMap<String, String>,
  pub datapoints: Vec<(T, i64)>
}
