use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::net::{UdpSocket, TcpStream};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, debug, warn};
use crate::transaction::TransactionId;
use super::{GraphiteConfig, GraphiteResponse};

macro_rules! format_metric {
  ($name:expr, $data:expr, $tags:expr, $tid:expr) => {
    {
      let time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(t) => t.as_secs(),
        Err(e) => {
          error!("[{}] udp time check failed because {}", $tid, e);
          return;
        },
      };
      format!("{};{} {} {}\n", $name, $tags, $data, time)
    }
  };
}

#[allow(unused)]
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

  fn publish_udp(&self, msg: String, tid: &TransactionId) {
    let socket = match UdpSocket::bind("127.0.0.1:9999") {
      Ok(s) => s,
      Err(e) => {
        error!("[{}] udp bind failed because {}", tid, e);
        return;
      },
    };

    match socket.send_to(msg.as_bytes(), &format!("{}:{}", self.config.address, self.config.graphite_port)) {
      Ok(_) => (),
      Err(e) => {
        error!("[{}] udp send failed because {}", tid, e);
      },
    };
  }

  fn publish_tcp(&self, msg: String, tid: &TransactionId) {
    let addr = format!("{}:{}", self.config.address, self.config.graphite_port);
    debug!("[{}] opening connection to '{}'", tid, addr);
    let mut socket = match TcpStream::connect(addr) {
      Ok(s) => s,
      Err(e) => {
        error!("[{}] tcp connect failed because {}", tid, e);
        return;
      },
    };
    
    debug!("[{}] pushing '{}'", tid, msg);
    match socket.write(msg.as_bytes()) {
      Ok(r) => {
        debug!("[{}] wrote '{}' bytes", tid, r)
      },
      Err(e) => {
        error!("[{}] tcp write failed because {}", tid, e);
      },
    };
  }

  pub fn publish_metric(&self, metric: &str, value: String, tid: &TransactionId, tags: String) {
    debug!("[{}] pushing metric {}", tid, metric);
    let msg = format_metric!(metric, value, tags, tid);
    if self.config.graphite_udp {
      self.publish_udp(msg, tid);
    } else {
      self.publish_tcp(msg, tid);
    }
  }

  pub async fn get_latest_metric<'a, T>(&self, metric: &str, by_tag: &str, tid: &TransactionId) -> HashMap<String, T>  // {
    where T : Default, T : serde::de::DeserializeOwned, T: Copy {
    let client = reqwest::Client::new();
    let url = format!("http://{}:{}/render?format=json&noNullPoints=true&from=-360s&target=seriesByTag('name={}')", self.config.address, self.config.api_port, metric);
    let mut ret = HashMap::new();

    match client.get(url)
      .send()
      .await {
        Ok(r) => {
          let text = match r.text().await {
            Ok(t) => t,
            Err(e) => {
              warn!("[{}] Failed to read graphite response because: {}", tid, e);
              return ret;
            },
          };
          let parsed = match serde_json::from_str::<GraphiteResponse<T>>(&text) {
            Ok(p) => p,
            Err(e) => {
              warn!("[{}] Failed to parse graphite response because: {}; response: {:?}", tid, e, text);
              return ret
            },
          };

          for metric in parsed {
            let tagged = match metric.tags.get(by_tag) {
              Some(t) => t,
              None => {
                warn!("[{}] Metric did not have tag! tag: {}", tid, by_tag);
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
          warn!("[{}] Failed to get graphite response because: {}", tid, e);
        },
      };
    ret
  }
}
