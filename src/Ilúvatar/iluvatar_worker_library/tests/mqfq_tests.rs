#[macro_use]
pub mod utils;

use crate::utils::sim_invoker_svc;
use iluvatar_library::logging::LocalTime;
use iluvatar_worker_library::services::{
    invocation::queueing::{
        gpu_mqfq::{FlowQ, MQState},
        EnqueuedInvocation,
    },
    registration::RegisteredFunction,
};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

fn build_gpu_env(overrun: f64) -> Vec<(String, String)> {
    vec![
        ("container_resources.gpu_resource.count".to_string(), "1".to_string()),
        (
            "invocation.mqfq_config.allowed_overrun".to_string(),
            format!("{}", overrun),
        ),
    ]
}

#[cfg(test)]
mod flowq_tests {
    use iluvatar_worker_library::services::containers::containermanager::ContainerManager;

    use super::*;

    fn item() -> Arc<EnqueuedInvocation> {
        let name = "test";
        let clock = LocalTime::new(&"clock".to_string()).unwrap();
        let rf = Arc::new(RegisteredFunction {
            function_name: name.to_string(),
            function_version: name.to_string(),
            fqdn: name.to_string(),
            image_name: name.to_string(),
            memory: 1,
            cpus: 1,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: iluvatar_library::types::Isolation::CONTAINERD,
            supported_compute: iluvatar_library::types::Compute::CPU,
        });
        Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            clock.now(),
        ))
    }

    async fn build_q(overrun: f64) -> (Option<impl Drop>, Arc<ContainerManager>, FlowQ) {
        let env = build_gpu_env(overrun);
        let (log, cfg, cm, _invoker, _reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let q = FlowQ::new(
            "test".to_string(),
            0.0,
            1.0,
            &cm,
            cfg.container_resources
                .gpu_resource
                .as_ref()
                .expect("GPU config was missing"),
            &cfg.invocation.mqfq_config.as_ref().expect("MQFQ config was missing"),
        );
        (log, cm, q)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn insert_set_active() {
        let (_log, _cm, mut q) = build_q(10.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item, 1.0);
        assert!(r, "single item requests VT update");
        assert_eq!(q.state, MQState::Active, "queue should be set active");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_pop_stays() {
        let (_log, _cm, mut q) = build_q(15.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item.clone(), 5.0);
        assert!(r, "single item requests VT update");
        let _r = q.push_flow(item.clone(), 10.0);
        assert_eq!(q.state, MQState::Active, "queue should be set active");
        let item2 = q.pop_flow(0.0);
        assert!(item2.is_some(), "must get item from queue");
        assert_eq!(item.queue_insert_time, item2.unwrap().invok.queue_insert_time);
        assert_eq!(q.start_time_virt, 15.0, "Queue start_time_virt was wrong");
        assert_eq!(q.state, MQState::Active, "inline queue should be active");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_pop_empty_is_inactive() {
        let (_log, _cm, mut q) = build_q(10.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item.clone(), 5.0);
        assert!(r, "single item requests VT update");
        assert_eq!(q.state, MQState::Active, "queue should be set active");
        let item2 = q.pop_flow(0.0);
        assert!(item2.is_some(), "must get item from queue");
        assert_eq!(item.queue_insert_time, item2.unwrap().invok.queue_insert_time);
        assert_eq!(q.state, MQState::Inactive, "inline queue should be active");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overrun_pop_causes_throttle() {
        let (_log, _cm, mut q) = build_q(10.0).await;
        assert_eq!(q.state, MQState::Inactive);
        let item = item();
        let r = q.push_flow(item.clone(), 20.0);
        assert!(r, "single item requests VT update");
        let r = q.push_flow(item.clone(), 30.0);
        assert_eq!(r, false, "second item does not request VT update");
        assert_eq!(q.state, MQState::Active, "queue should be set active");
        let item2 = q.pop_flow(0.0);
        assert!(item2.is_some(), "must get item from queue");
        assert_eq!(item.queue_insert_time, item2.unwrap().invok.queue_insert_time);
        assert_eq!(q.state, MQState::Throttled, "advanced queue should be throttled");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throttled_empty_q_made_active_grace_period() {
        let (_log, _cm, mut q) = build_q(10.0).await;
        let item = item();
        q.push_flow(item.clone(), 20.0);
        q.state = MQState::Throttled;
        q.last_serviced = OffsetDateTime::now_utc();
        q.set_idle_throttled(10.0);
        assert_eq!(q.state, MQState::Active);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn throttled_full_q_made_active() {
        let (_log, _cm, mut q) = build_q(10.0).await;
        let item = item();
        q.push_flow(item.clone(), 20.0);
        assert!(!q.queue.is_empty(), "queue not empty");

        q.state = MQState::Throttled;
        q.last_serviced = OffsetDateTime::now_utc() - Duration::seconds(30);
        q.set_idle_throttled(10.0);
        assert_eq!(q.state, MQState::Active);
    }
}
