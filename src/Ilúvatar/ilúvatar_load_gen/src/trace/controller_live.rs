use std::{collections::HashMap, time::{SystemTime, Duration}, path::Path, fs::File, io::Write};
use anyhow::Result;
use iluvatar_library::utils::{config::get_val, port::Port};
use clap::ArgMatches;
use tokio::{runtime::Builder, task::JoinHandle};
use crate::{utils::{controller_register, controller_invoke, VERSION, SuccessfulControllerInvocation, resolve_handles, save_result_json}, benchmark::BenchmarkStore, trace::{match_trace_to_img, CsvInvocation, prepare_function_args}};
use super::Function;

async fn register_functions(funcs: &HashMap<String, Function>, host: &String, port: Port, load_type: &str, func_data: Result<String>) -> Result<()> {
  let data = match load_type {
    "lookbusy" => HashMap::new(),
    "functions" => {
      let func_data = func_data?;
      let contents = std::fs::read_to_string(func_data).expect("Something went wrong reading the file");
      match serde_json::from_str::<BenchmarkStore>(&contents) {
        Ok(d) => {
          let mut data = HashMap::new();
          for (k, v) in d.data.iter() {
            let tot: f64 = v.warm_results.iter().sum();
            let avg_warm = tot / v.warm_results.len() as f64;
            data.insert(k.clone(), avg_warm);
          }
          data
        },
        Err(e) => anyhow::bail!("Failed to read and parse benchmark data! '{}'", e),
      }
    },
    _ => panic!("Bad invocation load type: {}", load_type),
  };
  for (_fid, func) in funcs.into_iter() {
    let image = match load_type {
      "lookbusy" => format!("docker.io/alfuerst/lookbusy-iluvatar-action:latest"),
      "functions" => match_trace_to_img(func, &data),
      _ => panic!("Bad invocation load type: {}", load_type),
    };
    println!("{}, {}", func.func_name, image);
    let _reg_dur = controller_register(&func.func_name, &VERSION, &image, func.mem_mb+50, host, port).await?;
  }
  Ok(())
}

pub fn controller_trace_live(main_args: &ArgMatches, sub_args: &ArgMatches) -> Result<()> {
  let trace_pth: String = get_val("input", &sub_args)?;
  let metadata_pth: String = get_val("metadata", &sub_args)?;
  let load_type: String = get_val("load-type", &sub_args)?;
  let func_data: Result<String> = get_val("function-data", &sub_args);
  let port: Port = get_val("port", &main_args)?;
  let host: String = get_val("host", &main_args)?;
  let metadata = super::load_metadata(metadata_pth)?;
  let threaded_rt = Builder::new_multi_thread()
      .enable_all()
      .build().unwrap();

  threaded_rt.block_on(register_functions(&metadata, &host, port, &load_type, func_data))?;

  let mut trace_rdr = csv::Reader::from_path(&trace_pth)?;
  let mut handles: Vec<JoinHandle<Result<SuccessfulControllerInvocation>>> = Vec::new();

  println!("starting live trace run");

  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invocation: CsvInvocation = result?;
    let func = metadata.get(&invocation.func_name).unwrap();
    let h_c = host.clone();
    let f_c = func.func_name.clone();
    let args = prepare_function_args(func, &load_type);
    
    loop {
      match start.elapsed() {
        Ok(t) => {
          let ms = t.as_millis() as u64;
          if ms >= invocation.invoke_time_ms {
            break;
          }
          std::thread::sleep(Duration::from_millis(ms/2));
        },
        Err(_) => (),
      }
    };
    handles.push(threaded_rt.spawn(async move {
      controller_invoke(&f_c, &VERSION, &h_c, port, Some(args)).await
    }));
  }
  let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;

  let pth = Path::new(&trace_pth);
  let output_folder: String = get_val("out", &main_args)?;
  let p = Path::new(&output_folder).join(format!("output-full{}.json", pth.file_stem().unwrap().to_str().unwrap()));
  save_result_json(p, &results)?;

  let p = Path::new(&output_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  let to_write = format!("success,function_name,was_cold,worker_duration_ms,invocation_duration_ms,code_duration_asec,e2e_duration_ms\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json of result because {}", e);
    }
  };

  for r in results {
    let to_write = format!("{},{},{},{},{},{},{}\n", r.controller_response.success, r.function_name, r.function_output.body.cold,
          r.controller_response.worker_duration_ms, r.controller_response.invoke_duration_ms, r.function_output.body.latency, r.client_latency_ms);
    match f.write_all(to_write.as_bytes()) {
      Ok(_) => (),
      Err(e) => {
        println!("Failed to write result because {}", e);
        continue;
      }
    };
  };

  Ok(())
}
