#[macro_use]
pub mod utils;

use crate::utils::build_test_services;
use iluvatar_library::transaction::TEST_TID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::{RegisterRequest, Runtime};
use rand::random;
use std::path::PathBuf;

fn make_tar(func: &str) -> Vec<u8> {
    tracing::info!("pwd: {:?}", std::env::current_dir());
    let pth = std::env::var("PY_FUNCS").expect("no py funcs dir");
    let mut pth = PathBuf::from(pth);
    pth.push("functions");
    pth.push(func);
    iluvatar_worker_library::tar_folder(pth, &"tar_tid".to_string()).expect("failed to tar folder")
}

fn basic_reg_req_docker(path: &str) -> RegisterRequest {
    RegisterRequest {
        function_name: "test".to_string(),
        function_version: random::<u16>().to_string(),
        cpus: 1,
        memory: 256,
        parallel_invokes: 1,
        image_name: "".to_string(),
        transaction_id: "testTID".to_string(),
        runtime: Runtime::Python3.into(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        container_server: 0,
        resource_timings_json: "".to_string(),
        system_function: false,
        code_zip: make_tar(path),
    }
}

mod tars {
    use super::*;
    use iluvatar_rpc::rpc::InvokeRequest;
    use rstest::rstest;

    #[iluvatar_library::live_test]
    async fn registration_works() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = build_test_services(None, None, None).await;
        reg.register(basic_reg_req_docker("chameleon"), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
    }

    #[iluvatar_library::live_test]
    async fn bad_code_reg_fails() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = build_test_services(None, None, None).await;
        let req = basic_reg_req_docker("../driver-hooks");
        assert_error!(reg.register(req, &TEST_TID).await, "failed installing dependencies", "");
    }

    #[iluvatar_library::live_test]
    #[rstest]
    #[case("chameleon")]
    #[case("dd")]
    #[case("float_operation")]
    #[case("lin_pack")]
    #[case("pyaes")]
    #[case("image_processing")]
    async fn invoke_works(#[case] code: &str) {
        let (_log, _cfg, _cm, invoker, reg, _, _) = build_test_services(None, None, None).await;
        let reg = reg
            .register(basic_reg_req_docker(code), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let req = InvokeRequest {
            function_name: reg.function_name.clone(),
            function_version: reg.function_version.clone(),
            json_args: "{}".to_string(),
            transaction_id: "testTID".to_string(),
        };
        let result = invoker
            .sync_invocation(reg, req.json_args, req.transaction_id)
            .await
            .unwrap();
        assert!(result.lock().completed);
        tracing::info!(res =? result.lock().worker_result, "completed");
        assert!(result.lock().worker_result.as_ref().unwrap().user_result.is_some());
    }
}
