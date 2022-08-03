pub mod round_robin;
pub mod least_loaded;

#[macro_export]
macro_rules! send_invocation {
  ($func:expr, $json_args:expr, $tid:expr, $worker_fact:expr, $health:expr, $worker:expr) => {
    {
      info!("[{}] invoking function {} on worker {}", $tid, &$func.fqdn, &$worker.name);

      let mut api = $worker_fact.get_worker_api(&$worker, $tid).await?;
      let result = match api.invoke($func.function_name.clone(), $func.function_version.clone(), $json_args, None, $tid.clone()).await {
        Ok(r) => r,
        Err(e) => {
          $health.schedule_health_check($health.clone(), $worker, $tid, Some(Duration::from_secs(1)));
          anyhow::bail!(e)
        },
      };
      debug!(tid=%$tid, json=%result.json_result, "invocation result");
      Ok(result.json_result)
    }
  };
}

#[macro_export]
macro_rules! prewarm {
  ($func:expr, $tid:expr, $worker_fact:expr, $health:expr, $worker:expr) => {
    {
      info!("[{}] prewarming function {} on worker {}", $tid, &$func.fqdn, &$worker.name);
      let mut api = $worker_fact.get_worker_api(&$worker, $tid).await?;
      let result = match api.prewarm($func.function_name.clone(), $func.function_version.clone(), None, None, None, $tid.clone()).await {
        Ok(r) => r,
        Err(e) => {
          $health.schedule_health_check($health.clone(), $worker, $tid, Some(Duration::from_secs(1)));
          anyhow::bail!(e)
        }
      };
      debug!(tid=%$tid, result=?result, "prewarm result");
      Ok(())
    }
  }
}

#[macro_export]
macro_rules! send_async_invocation {
  ($func:expr, $json_args:expr, $tid:expr, $worker_fact:expr, $health:expr, $worker:expr) => {
    {
      info!("[{}] invoking function async {} on worker {}", $tid, &$func.fqdn, &$worker.name);

      let mut api = $worker_fact.get_worker_api(&$worker, $tid).await?;
      let result = match api.invoke_async($func.function_name.clone(), $func.function_version.clone(), $json_args, None, $tid.clone()).await {
        Ok(r) => r,
        Err(e) => {
          $health.schedule_health_check($health.clone(), $worker, $tid, Some(Duration::from_secs(1)));
          anyhow::bail!(e)
        },
      };
      debug!(tid=%$tid, result=%result, "invocation result");
      Ok( (result, $worker) )
    }
  }
}