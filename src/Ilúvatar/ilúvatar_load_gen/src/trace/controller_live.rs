use std::{collections::HashMap, time::{SystemTime, Duration}, path::Path, fs::File, io::Write, sync::Arc};
use anyhow::Result;
use iluvatar_library::{utils::port::Port, logging::LocalTime, transaction::gen_tid};
use tokio::{runtime::Builder, task::JoinHandle};
use crate::utils::{controller_register, controller_invoke, VERSION, CompletedControllerInvocation, resolve_handles, save_result_json};
use crate::trace::{CsvInvocation, prepare_function_args, trace_utils::map_functions_to_prep};
use super::{Function, TraceArgs};

async fn register_functions(funcs: &HashMap<String, Function>, host: &String, port: Port) -> Result<()> {
  for (fid, func) in funcs.into_iter() {
    let image = func.image_name.as_ref().ok_or_else(|| anyhow::anyhow!("Unable to get image name for function '{}'", fid))?;
    println!("{}, {}", func.func_name, image);
    let _reg_dur = controller_register(&func.func_name, &VERSION, &image, func.mem_mb+50, host, port).await?;
  }
  Ok(())
}

pub fn controller_trace_live(args: TraceArgs) -> Result<()> {
  let mut metadata = super::load_metadata(args.metadata_csv)?;
  let threaded_rt = Builder::new_multi_thread()
      .enable_all()
      .build().unwrap();

  map_functions_to_prep(args.load_type, args.function_data, &mut metadata, 0, &args.input_csv)?;
  threaded_rt.block_on(register_functions(&metadata, &args.host, args.port))?;

  let mut trace_rdr = csv::Reader::from_path(&args.input_csv)?;
  let mut handles: Vec<JoinHandle<Result<CompletedControllerInvocation>>> = Vec::new();
  let clock = Arc::new(LocalTime::new(&gen_tid())?);

  println!("starting live trace run");

  let start = SystemTime::now();
  for result in trace_rdr.deserialize() {
    let invocation: CsvInvocation = result?;
    let func = metadata.get(&invocation.func_name).unwrap();
    let h_c = args.host.clone();
    let f_c = func.func_name.clone();
    let func_args = prepare_function_args(func, args.load_type);
    
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
    let clk_cln = clock.clone();
    handles.push(threaded_rt.spawn(async move {
      controller_invoke(&f_c, &VERSION, &h_c, args.port, Some(func_args), clk_cln).await
    }));
  }
  let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;

  let pth = Path::new(&args.input_csv);
  let p = Path::new(&args.out_folder).join(format!("output-full{}.json", pth.file_stem().unwrap().to_str().unwrap()));
  save_result_json(p, &results)?;

  let p = Path::new(&args.out_folder).join(format!("output-{}", pth.file_name().unwrap().to_str().unwrap()));
  let mut f = match File::create(p) {
    Ok(f) => f,
    Err(e) => {
      anyhow::bail!("Failed to create output file because {}", e);
    }
  };
  let to_write = format!("success,function_name,was_cold,worker_duration_us,invocation_duration_us,code_duration_asec,e2e_duration_us\n");
  match f.write_all(to_write.as_bytes()) {
    Ok(_) => (),
    Err(e) => {
      anyhow::bail!("Failed to write json of result because {}", e);
    }
  };

  for r in results {
    let to_write = format!("{},{},{},{},{},{},{}\n", r.controller_response.success, r.function_name, r.function_output.body.cold,
          r.controller_response.worker_duration_us, r.controller_response.invoke_duration_us, r.function_output.body.latency, r.client_latency_us);
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
