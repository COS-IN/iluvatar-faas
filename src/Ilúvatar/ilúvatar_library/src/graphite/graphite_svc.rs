use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::net::{UdpSocket, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, debug, warn, trace};
use crate::transaction::TransactionId;
use super::{GraphiteConfig, GraphiteResponse};

macro_rules! format_metric {
  ($name:expr, $data:expr, $tags:expr, $tid:expr) => {
    {
      let time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => t.as_secs(),
        Err(e) => {
          error!(tid=%$tid, error=%e, "udp time check failed");
          return;
        },
      };
      format!("{};{} {} {}\n", $name, $tags, $data, time)
    }
  };
}

#[derive(Debug)]
pub struct GraphiteService {
  config: Arc<GraphiteConfig>
}

impl GraphiteService {
  pub fn new(config: Arc<GraphiteConfig>) -> Self {
    GraphiteService {
      config
    }
  }

  pub fn boxed(config: Arc<GraphiteConfig>) -> Arc<Self> {
    Arc::new(GraphiteService::new(config))
  }

  fn publish_udp<T1, T2>(&self, metrics: &Vec<T1>, values: Vec<T2>, tid: &TransactionId, tags: T1)
    where T1: std::fmt::Display + std::fmt::Debug,
    T2: std::fmt::Display + std::fmt::Debug {
    
      let socket = match UdpSocket::bind("127.0.0.1:9999") {
      Ok(s) => s,
      Err(e) => {
        error!(tid=%tid, "udp bind failed because {}", e);
        return;
      },
    };
    for i in 0..metrics.len() {
      let metric = &metrics[i];
      let value = &values[i];
      let msg = format_metric!(metric, value, tags, tid);
      match socket.send_to(msg.as_bytes(), &format!("{}:{}", self.config.address, self.config.ingestion_port)) {
        Ok(_) => (),
        Err(e) => {
          error!(tid=%tid, "udp send failed because {}", e);
        },
      };
    }
  }

  fn publish_tcp<T1, T2>(&self, metrics: &Vec<T1>, values: Vec<T2>, tid: &TransactionId, tags: T1)
    where T1: std::fmt::Display + std::fmt::Debug,
    T2: std::fmt::Display + std::fmt::Debug {
      
    let addr = format!("{}:{}", self.config.address, self.config.ingestion_port);
    trace!(tid=%tid, "opening connection to '{}'", addr);
    let mut socket = match TcpStream::connect(addr) {
      Ok(s) => s,
      Err(e) => {
        error!(tid=%tid, "tcp connect failed because {}", e);
        return;
      },
    };
    for i in 0..metrics.len() {
      let metric = &metrics[i];
      let value = &values[i];
      let msg = format_metric!(metric, value, tags, tid);
      match socket.write(msg.as_bytes()) {
        Ok(r) => {
          trace!(tid=%tid, "wrote '{}' bytes",  r)
        },
        Err(e) => {
          error!(tid=%tid, "tcp write failed because {}",  e);
        },
      };  
    }
  }

  pub fn publish_metrics<T1, T2>(&self, metrics: &Vec<T1>, values: Vec<T2>, tid: &TransactionId, tags: T1)
  where T1: std::fmt::Display + std::fmt::Debug,
  T2: std::fmt::Display + std::fmt::Debug {
    if self.config.ingestion_udp {
      debug!(tid=%tid, metrics=?metrics, values=?values, "udp pushing metrics");
      self.publish_udp(metrics, values, tid, tags);
    } else {
      debug!(tid=%tid, metrics=?metrics, values=?values, "tcp pushing metrics");
      self.publish_tcp(metrics, values, tid, tags);
    }
  }

  pub fn publish_metric<T1, T2>(&self, metric: T1, value: T2, tid: &TransactionId, tags: T1)
    where T1: std::fmt::Display + std::fmt::Debug,
    T2: std::fmt::Display + std::fmt::Debug {

      self.publish_metrics(&vec![metric], vec![value], tid, tags)
  }

  pub async fn get_latest_metric<'a, T>(&self, metric: &str, by_tag: &str, tid: &TransactionId) -> HashMap<String, T>
    where T : Default, T : serde::de::DeserializeOwned, T: Copy {
    let client = reqwest::Client::new();
    let url = format!("http://{}:{}/render?format=json&noNullPoints=true&from=-360s&target=seriesByTag('name={}')", self.config.address, self.config.api_port, metric);
    debug!(tid=%tid, query=%url, "querying graphite render");

    let mut ret = HashMap::new();

    match client.get(url)
      .send()
      .await {
        Ok(r) => {
          let text = match r.text().await {
            Ok(t) => t,
            Err(e) => {
              warn!(tid=%tid, "Failed to read graphite response because: {}", e);
              return ret;
            },
          };
          let parsed = match serde_json::from_str::<GraphiteResponse<T>>(&text) {
            Ok(p) => p,
            Err(e) => {
              warn!(tid=%tid, "Failed to parse graphite response because: {}; response: {:?}", e, text);
              return ret
            },
          };

          for metric in parsed {
            let tagged = match metric.tags.get(by_tag) {
              Some(t) => t,
              None => {
                warn!(tid=%tid, "Metric did not have tag! tag: {}", by_tag);
                continue;
              },
            };

            let len = metric.datapoints.len();
            if len > 0 {
              let item = &metric.datapoints[len-1];
              ret.insert(tagged.clone(), item.0);
            } else {
              continue;
            }
          }
        },
        Err(e) =>{
          warn!(tid=%tid, "Failed to get graphite response because: {}", e);
        },
      };
    ret
  }
}
