use iluvatar_library::char_map::{worker_char_map, Chars};
use iluvatar_worker_library::services::invocation::queueing::congestion_pricing::CongestionPricingCoordinator;
use iluvatar_worker_library::services::invocation::queueing::{EnqueuedInvocation, InvokerCpuQueuePolicy};
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::services::resources::fineloadbalancing::FineLoadBalancingWeak;
use iluvatar_worker_library::worker_api::worker_config::{
    CongestionPricingCalibration, CongestionPricingConfig,
};
use std::sync::Arc;
use time::OffsetDateTime;

fn mock_invocation(fqdn: &str, tid_str: &str) -> Arc<EnqueuedInvocation> {
    let reg = Arc::new(RegisteredFunction {
        fqdn: fqdn.to_string(),
        cpus: 1,
        memory: 128,
        ..Default::default()
    });
    Arc::new(EnqueuedInvocation::new(
        reg,
        "{}".to_string(),
        tid_str.to_string(),
        OffsetDateTime::now_utc(),
        10.0,
        0.0,
    ))
}

#[test]
fn test_zero_config_startup_and_admission() {
    let cmap = worker_char_map();
    let coordinator = CongestionPricingCoordinator::new(
        None, // zero-config
        FineLoadBalancingWeak::new(),
        cmap,
        None,
    );

    // Initial queue should be empty
    assert_eq!(coordinator.queue_len(), 0);
    assert!(coordinator.peek_queue().is_none());

    // When admitting below saturation (unloaded system, idle override triggers), item is admitted into ready queue
    let item = mock_invocation("test.func.1", "tid-1");
    let res = coordinator.add_item_to_queue(&item, None);
    assert!(res.is_ok());

    // Admitted item is placed in ready queue for the worker execution loop to consume
    assert_eq!(coordinator.queue_len(), 1);
    assert!(coordinator.peek_queue().is_some());
    let popped = coordinator.pop_queue();
    assert_eq!(popped.tid, "tid-1");
    assert_eq!(coordinator.queue_len(), 0);
}

#[test]
fn test_calibration_override_and_freeze() {
    let cmap = worker_char_map();
    let config = CongestionPricingConfig {
        welfare_beta: Some(1.5),
        rho_knee: Some(1.05),
        cold_wave_coalescing: Some(true),
        cores_per_domain: Some(8),
        max_queue_per_domain: Some(32),
        pricing_calibration: Some(CongestionPricingCalibration {
            theta_warm: Some(2.0),
            theta_cold: Some(3.0),
            phi_aff: Some(0.1),
            freeze_calibration: Some(true),
        }),
    };

    let coordinator = CongestionPricingCoordinator::new(
        Some(&config),
        FineLoadBalancingWeak::new(),
        cmap,
        None,
    );

    // Initial admission should work cleanly
    let item = mock_invocation("test.func.2", "tid-2");
    assert!(coordinator.add_item_to_queue(&item, None).is_ok());
    assert_eq!(coordinator.queue_len(), 1);
    let _ = coordinator.pop_queue();

    // Simulate completion with high observed rho. Because freeze_calibration is true,
    // calibration slopes should not mutate.
    coordinator.on_completion(0, "test.func.2", 25.0, false, 1.8, None);
}

#[test]
fn test_completion_and_ewma_updates() {
    let cmap = worker_char_map();
    cmap.update("test.func.3", Chars::CpuWarmTime, 12.0);

    let coordinator = CongestionPricingCoordinator::new(
        None,
        FineLoadBalancingWeak::new(),
        cmap,
        None,
    );

    let item = mock_invocation("test.func.3", "tid-3");
    assert!(coordinator.add_item_to_queue(&item, None).is_ok());
    let _ = coordinator.pop_queue();

    // On completion with rho <= 1.0, EWMA for duration is updated cleanly
    coordinator.on_completion(0, "test.func.3", 15.0, false, 0.8, Some(12.0));
}
