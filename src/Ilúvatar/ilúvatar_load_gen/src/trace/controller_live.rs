use std::{collections::HashMap, time::{SystemTime, Duration},sync::Arc};
use anyhow::Result;
use iluvatar_library::{utils::port::Port, logging::LocalTime, transaction::gen_tid};
use tokio::{runtime::Builder, task::JoinHandle};
use crate::{utils::{controller_register, controller_invoke, VERSION, CompletedControllerInvocation, resolve_handles, controller_prewarm}, trace::trace_utils::save_controller_results};
use crate::trace::{CsvInvocation, prepare_function_args, trace_utils::map_functions_to_prep};
use super::{Function, TraceArgs};

async fn controller_live_register_functions(funcs: &HashMap<String, Function>, host: &String, port: Port) -> Result<()> {
  for (fid, func) in funcs.into_iter() {
    let image = func.image_name.as_ref().ok_or_else(|| anyhow::anyhow!("Unable to get image name for function '{}'", fid))?;
    println!("{}, {}", func.func_name, image);
    let _reg_dur = controller_register(&func.func_name, &VERSION, &image, func.mem_mb+50, host, port).await?;
  }
  Ok(())
}

async fn prewarm_funcs(funcs: &HashMap<String, Function>, host: &String, port: Port)  -> Result<()> {
  for (fid, func) in funcs.into_iter() {
    for _ in 0..func.prewarms.ok_or_else(|| anyhow::anyhow!("Function '{}' did not have a prewarm value, supply one or pass a benchmark file", fid))? {
      let _reg_dur = controller_prewarm(&func.func_name, &VERSION, host, port).await?;
    }
  }
  Ok(())
}

pub fn controller_trace_live(args: TraceArgs) -> Result<()> {
  let mut metadata = super::load_metadata(&args.metadata_csv)?;
  let threaded_rt = Builder::new_multi_thread()
      .enable_all()
      .build().unwrap();
  let client = match reqwest::Client::builder()
    .pool_max_idle_per_host(0)
    .pool_idle_timeout(None)
    .connect_timeout(Duration::from_secs(60))
    .build() {
      Ok(c) => Arc::new(c),
      Err(e) => panic!("Unable to build reqwest HTTP client: {:?}", e),
    };
  map_functions_to_prep(args.load_type, &args.function_data, &mut metadata, args.prewarms, &args.input_csv)?;
  threaded_rt.block_on(controller_live_register_functions(&metadata, &args.host, args.port))?;
  threaded_rt.block_on(prewarm_funcs(&metadata, &args.host, args.port))?;

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
    let http_c = client.clone();
    handles.push(threaded_rt.spawn(async move {
      controller_invoke(&f_c, &VERSION, &h_c, args.port, Some(func_args), clk_cln, http_c).await
    }));
  }
  let results = resolve_handles(&threaded_rt, handles, crate::utils::ErrorHandling::Print)?;
  save_controller_results(results, &args)
}
