#[macro_use]
pub mod utils;

use crate::utils::{short_sim_args, sim_args, sim_test_services};
use iluvatar_library::clock::get_global_clock;
use iluvatar_library::mindicator::Mindicator;
use iluvatar_library::transaction::{gen_tid, TEST_TID};
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::containers::containermanager::ContainerManager;
use iluvatar_worker_library::services::invocation::queueing::DeviceQueue;
use iluvatar_worker_library::services::resources::cpu::build_load_avg_signal;
use iluvatar_worker_library::services::{
    invocation::queueing::{
        gpu_mqfq::{FlowQ, MQState, MQFQ},
        EnqueuedInvocation,
    },
    registration::RegisteredFunction,
    resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker},
};
use rstest::rstest;
use std::sync::Arc;
use time::Duration;

static TIMEOUT_SEC: f64 = 2.0;

fn build_gpu_env(overrun: f64, timeout_sec: f64, mqfq_policy: &str) -> Vec<(String, String)> {
    vec![
        ("container_resources.gpu_resource.count".to_string(), "1".to_string()),
        ("invocation.queues.gpu".to_string(), "mqfq".to_string()),
        ("invocation.queue_policies.gpu".to_string(), mqfq_policy.to_string()),
        (
            "invocation.mqfq_config.allowed_overrun".to_string(),
            format!("{}", overrun),
        ),
        ("invocation.mqfq_config.ttl_sec".to_string(), timeout_sec.to_string()),
        ("invocation.mqfq_config.service_average".to_string(), "5.0".to_string()),
    ]
}

async fn build_flowq(overrun: f64) -> (Option<impl Drop>, Arc<ContainerManager>, FlowQ, Arc<Mindicator>) {
    let env = build_gpu_env(overrun, TIMEOUT_SEC, "mqfq");
    let (log, cfg, cm, _invoker, _reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
    let min = Mindicator::boxed(1);
    let q = FlowQ::new(
        "test".to_string(),
        0,
        0.0,
        1.0,
        &cm,
        cfg.container_resources
            .gpu_resource
            .as_ref()
            .expect("GPU config was missing"),
        cfg.invocation.mqfq_config.as_ref().expect("MQFQ config was missing"),
        &cmap,
        &min,
        &get_global_clock(&gen_tid()).unwrap(),
    );
    (log, cm, q, min)
}

async fn build_mqfq(
    overrun: f64,
    timeout_sec: f64,
    mqfq_policy: &str,
) -> (Option<impl Drop>, Arc<ContainerManager>, Arc<MQFQ>) {
    let env = build_gpu_env(overrun, timeout_sec, mqfq_policy);
    let (log, cfg, cm, _invoker, _reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
    let load_avg = build_load_avg_signal();
    let buff = Arc::new(iluvatar_library::ring_buff::RingBuffer::new(
        std::time::Duration::from_secs(2),
    ));
    let cpu = CpuResourceTracker::new(&cfg.container_resources.cpu_resource, load_avg, &TEST_TID).unwrap();
    let gpu = GpuResourceTracker::boxed(
        &cfg.container_resources.gpu_resource,
        &cfg.container_resources,
        &TEST_TID,
        &None,
        &cfg.status,
        &buff,
    )
    .await
    .unwrap();
    let q = MQFQ::new(
        cm.clone(),
        cmap,
        cfg.invocation.clone(),
        cpu,
        &gpu,
        &cfg.container_resources.gpu_resource,
        &TEST_TID,
    )
    .unwrap();
    (log, cm, q)
}

fn item() -> Arc<EnqueuedInvocation> {
    let name = gen_tid();
    let clock = get_global_clock(&name).unwrap();
    let rf = Arc::new(RegisteredFunction {
        function_name: name.to_string(),
        function_version: name.to_string(),
        fqdn: name.to_string(),
        image_name: name.to_string(),
        memory: 1,
        cpus: 1,
        parallel_invokes: 1,
        isolation_type: iluvatar_library::types::Isolation::DOCKER,
        supported_compute: iluvatar_library::types::Compute::GPU,
        ..Default::default()
    });
    Arc::new(EnqueuedInvocation::new(
        rf,
        sim_args().unwrap(),
        name.to_string(),
        clock.now(),
        0.0,
        0.0,
    ))
}

#[cfg(test)]
mod flowq_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn insert_set_active() {
        let (_log, _cm, mut q, _mindi) = build_flowq(10.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item, 1.0);
        assert!(r, "single item requests VT update");
        assert_eq!(q.state, MQState::Active, "queue should be set active");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_pop_stays() {
        let (_log, _cm, mut q, _mindi) = build_flowq(15.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item.clone(), 0.0);
        assert!(r, "single item requests VT update");
        let _r = q.push_flow(item.clone(), 0.0);
        assert_eq!(q.state, MQState::Active, "queue should be set active");
        let item2 = q.pop_flow();
        assert!(item2.is_some(), "must get item from queue");
        assert_eq!(item.queue_insert_time, item2.unwrap().invoke.queue_insert_time);
        assert_eq!(q.start_time_virt, 5.0, "Queue start_time_virt was wrong");
        assert_eq!(q.state, MQState::Active, "inline queue should be active");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_pop_empty_timeout_to_inactive() {
        let (_log, _cm, mut q, _mindi) = build_flowq(10.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item.clone(), 5.0);
        assert!(r, "single item requests VT update");
        assert_eq!(q.state, MQState::Active, "queue should be set active");
        let item2 = q.pop_flow();
        assert!(item2.is_some(), "must get item from queue");
        assert_eq!(item.queue_insert_time, item2.unwrap().invoke.queue_insert_time);
        assert_eq!(
            q.state,
            MQState::Active,
            "inline queue should be active immediately after pop"
        );
        // mark our 'invocation' as done
        q.mark_completed();
        q.pop_flow();
        tokio::time::sleep(std::time::Duration::from_secs(TIMEOUT_SEC as u64 + 1)).await;
        q.pop_flow();
        assert_eq!(
            q.state,
            MQState::Inactive,
            "inline queue should be ianctive after timeout"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overrun_pop_causes_throttle() {
        let (_log, _cm, mut q, mindi) = build_flowq(4.0).await;
        let id = mindi.add_procs(1) - 1;
        // force global VT to be 0
        mindi.insert(id, 0.0).unwrap();

        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item.clone(), 0.0);
        assert!(r, "single item requests VT update");
        let r = q.push_flow(item.clone(), 0.0);
        assert!(!r, "second item does not request VT update");
        assert_eq!(q.state, MQState::Active, "queue should be set active");
        let item2 = q.pop_flow();
        assert!(item2.is_some(), "must get item from queue");
        assert_eq!(item.queue_insert_time, item2.unwrap().invoke.queue_insert_time);
        assert_eq!(q.state, MQState::Throttled, "advanced queue should be throttled");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throttled_empty_q_made_active_grace_period() {
        let (_log, _cm, mut q, _mindi) = build_flowq(10.0).await;
        let item = item();
        q.push_flow(item.clone(), 20.0);
        q.state = MQState::Throttled;
        q.last_serviced = get_global_clock(&gen_tid()).unwrap().now();
        q.set_idle_throttled(10.0);
        assert_eq!(q.state, MQState::Active);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throttled_full_q_made_active() {
        let (_log, _cm, mut q, _mindi) = build_flowq(10.0).await;
        let item = item();
        q.push_flow(item.clone(), 20.0);
        assert!(!q.queue.is_empty(), "queue not empty");

        q.state = MQState::Throttled;
        q.last_serviced = get_global_clock(&gen_tid()).unwrap().now() - Duration::seconds(30);
        q.set_idle_throttled(10.0);
        assert_eq!(q.state, MQState::Active);
    }
}

#[cfg(test)]
mod mqfq_tests {
    use super::*;

    fn gpu_reg() -> RegisterRequest {
        RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        }
    }

    #[rstest]
    #[case("mqfq")]
    #[case("mqfq_longest")]
    #[case("mqfq_wait")]
    #[case("mqfq_sticky")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mqfq_works(#[case] mqfq_pol: &str) {
        let env = build_gpu_env(20.0, TIMEOUT_SEC, mqfq_pol);
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        let invoke = invoker
            .sync_invocation(func.clone(), short_sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::GPU, "Invoke compute must be GPU");
    }

    #[rstest]
    #[case("mqfq")]
    #[case("mqfq_longest")]
    #[case("mqfq_wait")]
    #[case("mqfq_sticky")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn function_active_until_timeout(#[case] mqfq_pol: &str) {
        let (_log, _cm, mqfq) = build_mqfq(20.0, TIMEOUT_SEC, mqfq_pol).await;
        let invoke1 = item();
        mqfq.enqueue_item(&invoke1).unwrap();
        let q = mqfq.mqfq_set.get(&invoke1.registration.fqdn).unwrap();
        assert_eq!(q.state, MQState::Active, "inline queue should be active");
        drop(q);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        loop {
            if invoke1.result_ptr.lock().completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        let q = mqfq.mqfq_set.get(&invoke1.registration.fqdn).unwrap();
        assert_eq!(q.state, MQState::Active, "inline queue should be active");
        drop(q);

        tokio::time::sleep(std::time::Duration::from_secs(TIMEOUT_SEC as u64 + 1)).await;
        let invoke2 = item();
        mqfq.enqueue_item(&invoke2).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let q = mqfq.mqfq_set.get(&invoke1.registration.fqdn).unwrap();
        assert_eq!(q.state, MQState::Inactive, "inline queue should be active");
        drop(q);
    }

    #[rstest]
    #[case("mqfq")]
    #[case("mqfq_longest")]
    #[case("mqfq_wait")]
    #[case("mqfq_sticky")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn function_inactive_zero_timeout(#[case] mqfq_pol: &str) {
        let (_log, _cm, mqfq) = build_mqfq(20.0, 0.0, mqfq_pol).await;
        let invoke1 = item();
        mqfq.enqueue_item(&invoke1).unwrap();
        let q = mqfq.mqfq_set.get(&invoke1.registration.fqdn).unwrap();
        assert_eq!(q.state, MQState::Active, "inline queue should be active");
        drop(q);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        loop {
            if invoke1.result_ptr.lock().completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        let q = mqfq.mqfq_set.get(&invoke1.registration.fqdn).unwrap();
        assert_eq!(
            q.state,
            MQState::Inactive,
            "inline queue should be inactive after only invoke is done and 0 timeout"
        );
        drop(q);
    }
}
