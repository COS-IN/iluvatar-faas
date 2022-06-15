use std::{sync::Arc, collections::HashMap};
use crate::containers::containermanager::ContainerManager;
use iluvatar_lib::{rpc::{InvokeRequest, InvokeAsyncRequest, InvokeResponse}, utils::calculate_fqdn};
use parking_lot::RwLock;
use std::time::SystemTime;
use anyhow::Result;
use reqwest;
use guid_create::GUID;
use log::*;
use crate::invocation::invoker_structs::AsyncInvoke;

#[derive(Debug)]
#[allow(unused)]
pub struct InvokerService {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: Arc<RwLock<HashMap<String, Arc<AsyncInvoke>>>>,
}

impl InvokerService {
    pub fn new(cont_manager: Arc<ContainerManager>) -> Self {
      InvokerService {
        cont_manager,
        async_functions: Arc::new(RwLock::new(HashMap::new()))
      }
    }

    pub async fn invoke(&self, request: &InvokeRequest) -> Result<(String, u64)> {
      InvokerService::invoke_internal(&request.function_name, &request.function_version, &request.json_args, &self.cont_manager).await
    }

    async fn invoke_internal(function_name: &String, function_version: &String, json_args: &String, 
      cont_manager: &Arc<ContainerManager>) -> Result<(String, u64)> {
      let fqdn = calculate_fqdn(&function_name, &function_version);
      match cont_manager.acquire_container(&fqdn).await? {
        Some(ctr_lock) => 
        {
          let client = reqwest::Client::new();
          let start = SystemTime::now();
          let result = client.post(&ctr_lock.container.invoke_uri)
            .body(json_args.to_owned())
            .header("Content-Type", "application/json")
            .send()
            .await?;
          let duration = start.elapsed()?.as_millis() as u64;
          let data = result.text().await?;
          Ok((data, duration))
        },
        None => anyhow::bail!("Unable to acquire a container for function '{}'", &fqdn),
      }
    }

    pub fn invoke_async(&self, request: Arc<InvokeAsyncRequest>) -> Result<String> {
      let cookie = GUID::rand().to_string();

      let containers = self.cont_manager.clone();
      let request = request.clone();
      let async_functions = self.async_functions.clone();
      let cookie_clone = cookie.clone();

      let invoke = Arc::new(AsyncInvoke {
        result: None,
        completed: false,
        duration: 0,
      });
      {
        let mut async_functions_lock = async_functions.write();
        async_functions_lock.insert(cookie_clone, invoke);
      }
      let cookie_clone = cookie.clone();

      let _handle: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::task::spawn(async move {
        debug!("Launching Async invocation for cookie '{}'", cookie_clone);
        let result = match InvokerService::invoke_internal(&request.function_name, &request.function_version, 
                  &request.json_args, &containers).await {
                    Ok(res) => res,
                    Err(e) => ( format!("{{ 'Error': '{}' }}", e), 0),
                };
        let invoke = Arc::new(AsyncInvoke {
          result: Some(result.0),
          completed: true,
          duration: result.1,
        });
        debug!("Async invocation completed for cookie '{}'", cookie_clone);
        let mut async_functions_lock = async_functions.write();
        async_functions_lock.insert(cookie_clone, invoke);
        Ok(())
      });
  
      Ok(cookie)
    }

    pub fn get_async_entry(&self, cookie: &String) -> Option<Arc<AsyncInvoke>> {
      let async_functions_lock = self.async_functions.read();
      let i = async_functions_lock.get(cookie);
      match i {
          Some(i) => Some(i.to_owned()),
          None => None,
      }
    }

    pub fn invoke_async_check(&self, cookie: &String) -> Result<InvokeResponse> {
      let i = match self.get_async_entry(cookie) {
        Some(i) => i,
        None => return Ok(InvokeResponse {
          json_result: "{ 'Error': 'Invocation not found' }".to_string(),
          success: false,
          duration_ms: 0
        }),
      };

      if i.completed {
        let ret = i.result.to_owned();
        let mut async_functions_lock = self.async_functions.write();
        async_functions_lock.remove(cookie);
        return Ok(InvokeResponse {
          json_result: match ret {
            Some(json) => json,
            None =>  {
              error!("Completed async invocation didn't have a result??");
              "{ 'Error': 'No result was captured' }".to_string()
            },
          },
          success: true,
          duration_ms: i.duration,
        });
      }
      Ok(InvokeResponse {
        json_result: "{ 'Status': 'Invocation not completed' }".to_string(),
        success: false,
        duration_ms: 0
      })
    }
}
