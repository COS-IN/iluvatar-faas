use std::{collections::HashMap, time::Duration, cmp::min};
use anyhow::Result;
use iluvatar_library::{utils::{port::Port}, transaction::TransactionId, logging::LocalTime};
use tokio::{runtime::Runtime, task::JoinHandle};
use crate::{utils::{worker_register, VERSION, worker_prewarm}, benchmark::BenchmarkStore, trace::{safe_cmp, CsvInvocation}};
use super::Function;

#[allow(unused)]
pub enum RegisterTarget {
  LiveWorker,
  LiveController,
  SimWorker,
  SimController,
}

fn compute_prewarms(f: &Function, default_prewarms: u32) -> u32 {
  if let Some(iat) = f.mean_iat {
    return f64::ceil(f.warm_dur_ms as f64 / iat) as u32;
  }
  default_prewarms
}

fn map_from_benchmark(funcs: &HashMap<String, Function>, bench: &BenchmarkStore, 
                      default_prewarms: u32, trace_pth: &String) -> Result<HashMap<String, (String, u32)>> {
  let mut data = Vec::new();
  for (k, v) in bench.data.iter() {
    let tot: f64 = v.warm_results.iter().sum();
    let avg_cold = v.cold_invoke_duration_us.iter().sum::<u128>() as f64 / v.cold_invoke_duration_us.len() as f64;
    let avg_warm = tot / v.warm_results.len() as f64;
                          // Cold uses E2E duration because of the cold start time needed
    data.push(( k.clone(), avg_warm*1000.0, avg_cold/1000.0) );
  }
  
  let mut invocations = HashMap::new();
  let mut trace_rdr = match csv::Reader::from_path(trace_pth) {
    Ok(r) => r,
    Err(e) => anyhow::bail!("Unable to open trace csv file '{}' because of error '{}'", trace_pth, e),
  };
  for result in trace_rdr.deserialize::<CsvInvocation>() {
    match result {
      Ok(i) => {
        if i.invoke_time_ms > 5*60*1000 {
          break;
        }
        match invocations.get_mut(&i.func_name) {
          Some(entry) => *entry += 1,
          None => {invocations.insert(i.func_name, 1);},
        }
      },
      Err(e) => anyhow::bail!("Error deserializing csv invocation: {}", e),
    };
  }

  let mut ret = HashMap::new();
  let mut total_prewarms=0;
  for (_fname, func) in funcs.iter(){
    let chosen = match data.iter().min_by(|a, b| safe_cmp(&a.1,&b.1)) {
      Some(n) => n,
      None => panic!("failed to get a minimum func from {:?}", data),
    };
    let mut chosen_name = chosen.0.clone();
    let mut chosen_warm_time = chosen.1;
    let mut chosen_cold_time = chosen.2;
  
    for (name, avg_warm, avg_cold) in data.iter() {
      if func.warm_dur_ms as f64 >= *avg_warm && chosen_warm_time < *avg_warm {
        chosen_name = name.clone();
        chosen_warm_time = *avg_warm;
        chosen_cold_time = *avg_cold;
      }
    }
    let little_prewarms = if let Some(invokes) = invocations.get(&func.func_name) {
      f64::ceil( (*invokes as f64 / 60.0) / (1000.0 / chosen_warm_time) ) as u32
    } else {
      0
    };

    let mut prewarms = match func.mean_iat {
      Some(iat) => {
        let prewarms = f64::ceil(chosen_cold_time as f64 / iat) as u32;
        // println!("{} -> {} / {} = {}", chosen_name, chosen_cold_time, iat, prewarms);
        min(prewarms, default_prewarms+10)
      },
      None => default_prewarms,
    };
    prewarms = std::cmp::max(prewarms, little_prewarms);
    println!("'{}' little prewarms {} vs IAT {} ", &func.func_name, little_prewarms, prewarms);
    total_prewarms += prewarms;
    ret.insert( func.func_name.clone(), (format!("docker.io/alfuerst/{}-iluvatar-action:latest", chosen_name), prewarms) );
    println!("{} mapped to function '{}'", &func.func_name, chosen_name);
  }
  println!("A total of {} prewarmed containers", total_prewarms);
  Ok(ret)
}

fn map_from_lookbusy(funcs: &HashMap<String, Function>, default_prewarms: u32) -> Result<HashMap<String, (String, u32)>> {
  let mut ret = HashMap::new();
  for (fname, func) in funcs.iter() {
    ret.insert(fname.clone(), ("docker.io/alfuerst/lookbusy-iluvatar-action:latest".to_string(), compute_prewarms(func, default_prewarms)) );
  }
  Ok(ret)
}

fn map_from_metadata(funcs: &HashMap<String, Function>, default_prewarms: u32) -> Result<HashMap<String, (String, u32)>> {
  let mut ret = HashMap::new();
  for (fname, func) in funcs.iter() {
    ret.insert(fname.clone(), (func.image_name.as_ref().ok_or_else(|| anyhow::anyhow!("Function '{}' did not have a matching image listed", fname))?.clone(), compute_prewarms(func, default_prewarms)) );
  }
  Ok(ret)
}

pub fn map_functions_to_prep(load_type: &str, func_json_data: Result<String>, funcs: &HashMap<String, Function>, 
                            default_prewarms: u32, trace_pth: &String) -> Result<HashMap<String, (String, u32)>> {
  match load_type {
    "lookbusy" => { return map_from_lookbusy(funcs, default_prewarms); },
    "functions" => {
      if let Ok(func_json_data) = func_json_data {
        // Choosing functions from json file benchmark data
        let contents = std::fs::read_to_string(func_json_data).expect("Something went wrong reading the benchmark file");
        match serde_json::from_str::<BenchmarkStore>(&contents) {
          Ok(d) => {
            return map_from_benchmark(funcs, &d, default_prewarms, trace_pth);
          },
          Err(e) => anyhow::bail!("Failed to read and parse benchmark data! '{}'", e),
        }
      } else {
        return map_from_metadata(funcs, default_prewarms);
      }
    }
    _ => anyhow::bail!("Unknown load type '{}'", load_type)
  }
}

fn live_prewarm_functions(prewarm_data: &HashMap<String, (String, u32)>, host: &String, port: Port, rt: &Runtime) -> Result<()> {
  let mut prewarm_calls = vec![];
  for (func_name, (_image, count)) in prewarm_data.iter() {
    println!("{} prewarming {} containers for function '{}'", LocalTime::new(&"PREWARM_LOAD_GEN".to_string())?.now_str()?, count, func_name);
    for i in 0..*count {
      let tid = format!("{}-{}-prewarm", i, &func_name);
      let h_c = host.clone();
      let f_c = func_name.clone();
      prewarm_calls.push(async move { 
        let mut errors="Prewarm errors:".to_string();
        let mut it = (1..4).into_iter().peekable();
        while let Some(i) = it.next() {
          match worker_prewarm(&f_c, &VERSION, &h_c, port, &tid).await {
            Ok((_s, _prewarm_dur)) => break,
            Err(e) => { 
              errors = format!("{} iteration {}: '{}';\n", errors, i, e);
              if it.peek().is_none() {
                anyhow::bail!("prewarm failed because {}", errors)
              }
            },
          };
        }
        Ok(())
      });
    }
  }
  while prewarm_calls.len() > 0 {
    let mut handles = vec![];
    for _ in 0..4 {
      match prewarm_calls.pop() {
        Some(p) => handles.push(rt.spawn(p)),
        None => break,
      }
      std::thread::sleep(Duration::from_millis(10));
    }
    for handle in handles {
      rt.block_on(handle)??;
    }
  }
  Ok(())
}

pub fn prepare_functions(target: RegisterTarget, funcs: &HashMap<String, Function>, host: &String, 
                          port: Port, load_type: &str, func_data: Result<String>, rt: &Runtime, 
                          prewarms: u32, trace_pth: &String) -> Result<()> {
  match target {
    RegisterTarget::LiveWorker => live_prepare_worker(&funcs, host, port, load_type, func_data, prewarms, rt, trace_pth),
    RegisterTarget::LiveController => todo!(),
    RegisterTarget::SimWorker => todo!(),
    RegisterTarget::SimController => todo!(),
  }
}

fn live_prepare_worker(funcs: &HashMap<String, Function>, host: &String, port: Port, load_type: &str, 
                      func_data: Result<String>, prewarms: u32, rt: &Runtime, trace_pth: &String) -> Result<()> {
  let prep_data = map_functions_to_prep(load_type, func_data, &funcs, prewarms, trace_pth)?;
  wait_reg(&funcs, &prep_data, load_type, rt, port, host)?;

  live_prewarm_functions(&prep_data, host, port, rt)?;
  Ok(())
}

fn wait_reg(funcs: &HashMap<String, Function>, prep_data: &HashMap<String, (String, u32)>, load_type: &str, rt: &Runtime, port: Port, host: &String) -> Result<()> {
  let mut func_iter = funcs.into_iter();
  loop {
    let mut handles: Vec<JoinHandle<Result<(String, Duration, TransactionId)>>> = Vec::new();
    for _ in 0..40 {
      let (id, func) = match func_iter.next() {
        Some(d) => d,
        None => {
          return Ok(());
        },
      };
      let mb = match load_type {
        "lookbusy" => func.mem_mb+50,
        "functions" => 512,
        _ => panic!("Bad invocation load type: {}", load_type),
      };
      let f_c = func.func_name.clone();
      let h_c = host.clone();
      let image = prep_data.get(id).ok_or_else(|| anyhow::anyhow!("Unable to get prep data for function '{}'", id))?.0.clone();
      handles.push(rt.spawn(async move { worker_register(f_c, &VERSION, image, mb+50, h_c, port).await }));
    }
    for h in handles {
      let (_s,_d,_s2) = rt.block_on(h)??;
    }
  }
}
