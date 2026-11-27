#[macro_use]
pub mod utils;

use crate::utils::build_test_services;
use iluvatar_finesched::SchedGroup;
use iluvatar_library::char_map::{worker_char_map, Chars};
use iluvatar_library::transaction::{gen_tid, TEST_TID};
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::containers::simulator::simstructs::SimulatorContainer;
use iluvatar_worker_library::services::containers::structs::{Container, ContainerState};
use iluvatar_worker_library::services::invocation::queueing::posted_price::{
    domain_cpu_price, invocation_price, DomainLoadEstimate, DomainLoadEstimator, InvocationStateKind,
    PeltBiasObserverEstimator, PostedPriceCoordinator, UnitFootprintEstimator, UtilWeightedEstimator,
    DEFAULT_C_MIG_MS, MIGRATION_COST_BOOT_FRACTION,
};
use iluvatar_worker_library::services::invocation::queueing::{EnqueuedInvocation, InvokerCpuQueuePolicy};
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::services::resources::cpu::{build_load_avg_signal, CpuResourceTracker};
use iluvatar_worker_library::services::resources::fineloadbalancing::FineLoadBalancingWeak;
use iluvatar_worker_library::worker_api::worker_config::{
    CPUResourceConfig, CpuQueuePolicy, FineLoadBalancingConfig, LoadEstimatorKind, PostedPriceConfig,
};
use std::str::FromStr;
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
fn test_posted_price_cli_and_enum_parsing() {
    assert_eq!(
        CpuQueuePolicy::from_str("posted_price").unwrap(),
        CpuQueuePolicy::PostedPrice
    );
    assert_eq!(
        CpuQueuePolicy::from_str("congestion_pricing").unwrap(),
        CpuQueuePolicy::PostedPrice
    );
    let serialized = serde_json::to_string(&CpuQueuePolicy::PostedPrice).unwrap();
    assert_eq!(serialized, "\"posted_price\"");
}

/// 1. Unit parity: Verify `private_cost`, `externality_cost`, `state_cost`, and `B_i`
/// scale consistently across short (`s_i = 2 ms`) and long (`s_i = 400 ms`) functions under `beta = 0.5`.
#[test]
fn test_unit_parity_short_vs_long_under_beta_0p5() {
    let beta = 0.5;
    let cores = 6.0;
    let est = DomainLoadEstimate {
        effective_rho: 0.5,
        active_weight: 1.2,
    };
    let unit = domain_cpu_price(est, cores);

    let short_s = 2.0;
    let long_s = 400.0;

    let short_remote = invocation_price(
        0,
        short_s,
        beta,
        est,
        (Some(0.5), Some(3.0)),
        cores,
        InvocationStateKind::WarmRemote {
            c_mig_ms: DEFAULT_C_MIG_MS,
        },
    );
    let long_remote = invocation_price(
        0,
        long_s,
        beta,
        est,
        (Some(0.5), Some(3.0)),
        cores,
        InvocationStateKind::WarmRemote {
            c_mig_ms: DEFAULT_C_MIG_MS,
        },
    );

    // s_i ratio is 200; under beta = 0.5:
    // - w_0(400) / w_0(2) = (400 / 2)^(-0.5) = 1 / sqrt(200)
    // - B_i(400) / B_i(2) = (400 / 2)^(0.5) = sqrt(200)
    // - private_cost / B_i == sigma_d (identical across all s_i)
    // - externality_cost(400) / externality_cost(2) == 400 / 2 = 200 (pure CPU time scaling)
    // - state_cost(400) / state_cost(2) == w_0(400) / w_0(2) = 1 / sqrt(200)
    let sqrt_200 = (200.0_f64).sqrt();
    assert!((long_remote.w_0 / short_remote.w_0 - 1.0 / sqrt_200).abs() < 1e-12);
    assert!((long_remote.benefit_b_i / short_remote.benefit_b_i - sqrt_200).abs() < 1e-12);
    assert!((short_remote.private_cost / short_remote.benefit_b_i - unit.sigma_d).abs() < 1e-12);
    assert!((long_remote.private_cost / long_remote.benefit_b_i - unit.sigma_d).abs() < 1e-12);
    assert!((long_remote.private_cost / short_remote.private_cost - sqrt_200).abs() < 1e-12);
    assert!((long_remote.externality_cost / short_remote.externality_cost - 200.0).abs() < 1e-12);
    assert!((long_remote.state_cost / short_remote.state_cost - 1.0 / sqrt_200).abs() < 1e-12);

    // Also verify ColdBoot state_cost parity (`w_0 * (C_boot(d) - Gamma_create)`)
    let cold_state = InvocationStateKind::ColdBoot {
        c_boot_ms: 275.0,
        gamma_create_ms: 250.0,
    };
    let short_cold = invocation_price(0, short_s, beta, est, (None, None), cores, cold_state);
    let long_cold = invocation_price(0, long_s, beta, est, (None, None), cores, cold_state);
    assert!((short_cold.state_cost - short_cold.w_0 * 25.0).abs() < 1e-12);
    assert!((long_cold.state_cost - long_cold.w_0 * 25.0).abs() < 1e-12);
    assert!((long_cold.state_cost / short_cold.state_cost - 1.0 / sqrt_200).abs() < 1e-12);
    assert!((MIGRATION_COST_BOOT_FRACTION - 0.01).abs() < 1e-12);
}

/// 2. Monotonicity: Verify `domain_cpu_price` is strictly increasing in `effective_rho`
/// from `0.0` upward with no flat dead zone and superlinear externality growth near `1.0+`.
#[test]
fn test_domain_cpu_price_strict_monotonicity_without_dead_zone() {
    let cores = 6.0;
    let zero_price = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 0.0,
            active_weight: 0.0,
        },
        cores,
    );
    assert_eq!(zero_price.sigma_d, 0.0);
    assert_eq!(zero_price.p_d, 0.0);

    let grid = [
        0.0, 0.001, 0.01, 0.05, 0.10, 0.20, 0.35, 0.50, 0.65, 0.80, 0.95, 1.00, 1.15, 1.35, 1.60,
    ];
    let mut prev_sigma = -1.0;
    let mut prev_p = -1.0;
    for &rho in &grid {
        let unit = domain_cpu_price(
            DomainLoadEstimate {
                effective_rho: rho,
                active_weight: 1.0,
            },
            cores,
        );
        assert!(
            unit.sigma_d > prev_sigma,
            "sigma_d must strictly increase at rho={rho}: {} <= {}",
            unit.sigma_d,
            prev_sigma
        );
        assert!(
            unit.p_d > prev_p,
            "p_d must strictly increase at rho={rho}: {} <= {}",
            unit.p_d,
            prev_p
        );
        prev_sigma = unit.sigma_d;
        prev_p = unit.p_d;
    }

    // Verify superlinear rise in p_d: slope over [0.8, 1.2] vastly exceeds slope over [0.0, 0.4].
    let p_0 = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 0.0,
            active_weight: 1.0,
        },
        cores,
    )
    .p_d;
    let p_04 = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 0.4,
            active_weight: 1.0,
        },
        cores,
    )
    .p_d;
    let p_08 = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 0.8,
            active_weight: 1.0,
        },
        cores,
    )
    .p_d;
    let p_12 = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 1.2,
            active_weight: 1.0,
        },
        cores,
    )
    .p_d;
    assert!((p_12 - p_08) > 5.0 * (p_04 - p_0));
}

/// 3. Locality vs. spill: Verify warm-domain preference holds when balanced and spills
/// to a quieter domain once `sigma_d0 - sigma_d*` savings exceed `C_mig / s_i`.
#[test]
fn test_locality_vs_spill_threshold() {
    #[derive(Default)]
    struct FixedEstimator {
        loads: Vec<DomainLoadEstimate>,
    }
    impl DomainLoadEstimator for FixedEstimator {
        fn on_dispatch(&mut self, _d: usize, _fqdn: &str, _w: f64) -> f64 {
            1.0
        }
        fn on_complete(&mut self, _d: usize, _fqdn: &str, _w: f64, _fp: f64) {}
        fn on_boot_start(&mut self, _d: usize, _fqdn: &str) {}
        fn on_boot_end(&mut self, _d: usize, _fqdn: &str) {}
        fn on_pelt_sample(&mut self, _d: usize, _u: f64, _r: f64) {}
        fn predict(&self, domain: usize) -> DomainLoadEstimate {
            self.loads.get(domain).copied().unwrap_or_default()
        }
    }

    let cmap = worker_char_map();
    // 50 ms warm service time (0.050 s in WorkerCharMap); C_mig / s_i = 2.5 / 50.0 = 0.05.
    cmap.update("bfs-0.0.1", Chars::CpuWarmTime, 0.050);
    let cfg = PostedPriceConfig {
        welfare_beta: Some(0.5),
        cores_per_domain: Some(6),
        c_mig_ms: Some(2.5),
        ..Default::default()
    };

    // Case A: Balanced load (rho = 0.30 on all domains), warm container on d0 -> prefers d0.
    let balanced = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap.clone(),
        None,
        4,
        Box::new(FixedEstimator {
            loads: vec![
                DomainLoadEstimate {
                    effective_rho: 0.30,
                    active_weight: 0.0,
                };
                4
            ],
        }),
    );
    let (ranked_bal, _) = balanced.rank_domains_with_state("bfs-0.0.1", true, &[0], 1);
    assert_eq!(ranked_bal[0], 0, "balanced domains must prefer warm domain d0");

    // Case B: Slight imbalance below C_mig / s_i = 0.05 (rho_d0 = 1/6 -> sigma_d0 = 0.0207 < 0.05, rho_d1 = 0.0) -> stays on d0.
    let sub_threshold = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap.clone(),
        None,
        4,
        Box::new(FixedEstimator {
            loads: vec![
                DomainLoadEstimate {
                    effective_rho: 1.0 / 6.0,
                    active_weight: 0.0,
                },
                DomainLoadEstimate {
                    effective_rho: 0.0,
                    active_weight: 0.0,
                },
                DomainLoadEstimate {
                    effective_rho: 0.5,
                    active_weight: 0.0,
                },
                DomainLoadEstimate {
                    effective_rho: 0.5,
                    active_weight: 0.0,
                },
            ],
        }),
    );
    let sigma_1_6 = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 1.0 / 6.0,
            active_weight: 0.0,
        },
        6.0,
    )
    .sigma_d;
    assert!(sigma_1_6 < 2.5 / 50.0);
    let (ranked_keep, _) = sub_threshold.rank_domains_with_state("bfs-0.0.1", true, &[0], 1);
    assert_eq!(ranked_keep[0], 0, "must keep warm locality when sigma gap < C_mig / s_i");

    // Case C: Imbalance exceeds C_mig / s_i = 0.05 (rho_d0 = 2/6 -> sigma_d0 = 0.0509 > 0.05, rho_d1 = 0.0) -> spills to d1!
    let over_threshold = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap,
        None,
        4,
        Box::new(FixedEstimator {
            loads: vec![
                DomainLoadEstimate {
                    effective_rho: 2.0 / 6.0,
                    active_weight: 0.0,
                },
                DomainLoadEstimate {
                    effective_rho: 0.0,
                    active_weight: 0.0,
                },
                DomainLoadEstimate {
                    effective_rho: 0.5,
                    active_weight: 0.0,
                },
                DomainLoadEstimate {
                    effective_rho: 0.5,
                    active_weight: 0.0,
                },
            ],
        }),
    );
    let sigma_2_6 = domain_cpu_price(
        DomainLoadEstimate {
            effective_rho: 2.0 / 6.0,
            active_weight: 0.0,
        },
        6.0,
    )
    .sigma_d;
    assert!(sigma_2_6 > 2.5 / 50.0);
    let (ranked_spill, _) = over_threshold.rank_domains_with_state("bfs-0.0.1", true, &[0], 1);
    assert_eq!(ranked_spill[0], 1, "must spill to quieter domain d1 once sigma_d0 - sigma_d1 > C_mig / s_i");
}

/// 4. Atomic spill & 1:1 accounting: Verify `acquire_cores(&[d0, d1, ...])` spills to `d1`
/// when `d0` is full and updates `on_dispatch`, `last_domain`, and BPF assignment on `d1`.
#[iluvatar_library::sim_test]
async fn test_atomic_spill_and_1to1_domain_accounting() {
    let (_log, _cfg, _cm, _invoker, reg_svc, cmap, _gpu) = build_test_services(None, None, None).await;
    let mut flb_config = FineLoadBalancingConfig::default();
    flb_config.dispatchpolicy = "posted_price".to_string();
    flb_config.testing = 1;
    // 4 domains, 1 core each (so 1 active task fills d0 completely)
    flb_config.preallocated_groups.groups = (0..4)
        .map(|core| SchedGroup {
            cores: vec![core],
            ..Default::default()
        })
        .collect();
    let cpu_config = Arc::new(CPUResourceConfig {
        count: 4,
        ..Default::default()
    });
    let tracker = CpuResourceTracker::new(
        &cpu_config,
        build_load_avg_signal(),
        &TEST_TID,
        cmap.clone(),
        Some(Arc::new(flb_config)),
    )
    .unwrap();
    let flb = tracker.fineloadbalancing().unwrap();

    let pp_cfg = PostedPriceConfig {
        welfare_beta: Some(0.5),
        cores_per_domain: Some(1),
        ..Default::default()
    };
    let coordinator = PostedPriceCoordinator::new(
        Some(&pp_cfg),
        Arc::downgrade(&flb),
        cmap.clone(),
        None,
    );

    let func_reg = reg_svc
        .register(
            RegisterRequest {
                function_name: gen_tid(),
                function_version: "1".to_string(),
                cpus: 1,
                memory: 128,
                parallel_invokes: 1,
                image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
                transaction_id: "reg-tid".to_string(),
                compute: Compute::CPU.bits(),
                isolate: Isolation::DOCKER.bits(),
                ..Default::default()
            },
            &TEST_TID,
        )
        .await
        .unwrap();
    cmap.update(&func_reg.fqdn, Chars::CpuWarmTime, 0.050);

    // Occupy d0 completely (`assigned_cores == 1`).
    let blocker_tid = "blocker-d0".to_string();
    let (dom0, blocker_permit) = tracker
        .acquire_cores(&func_reg, &blocker_tid, &[0, 1, 2, 3])
        .await
        .unwrap()
        .expect("d0 must succeed when empty");
    assert_eq!(dom0, 0);

    // Now request `[0, 1, 2, 3]`: d0 is full, so `acquire_cores` must atomically spill to d1!
    let spill_tid = "spill-to-d1".to_string();
    let (_, snap) = coordinator.rank_domains_with_state(&func_reg.fqdn, true, &[0], 1);
    let (actual_domain, spill_permit) = tracker
        .acquire_cores(&func_reg, &spill_tid, &[0, 1, 2, 3])
        .await
        .unwrap()
        .expect("must spill to d1");
    assert_eq!(actual_domain, 1);

    // Touchpoint 3: 1:1 attribution on d1 across coordinator, container `last_domain`, and BPF map!
    let lifecycle = coordinator.begin_dispatch(&spill_tid, &func_reg.fqdn, actual_domain, Some(&snap), None);
    let sim_ctr: Container = Arc::new(
        SimulatorContainer::new(
            &spill_tid,
            "ctr-spill-1".to_string(),
            &func_reg.fqdn,
            &func_reg,
            ContainerState::Warm,
            Isolation::DOCKER,
            Compute::CPU,
            None,
        )
        .unwrap(),
    );
    sim_ctr.set_last_domain(Some(actual_domain));
    tracker
        .cgroup_assigned_to_function(
            sim_ctr.cgroup_id(),
            &spill_tid,
            func_reg.clone(),
            Some(actual_domain),
            Some(lifecycle.s_i_ms),
            Some(lifecycle.w_0),
        )
        .unwrap();

    assert_eq!(coordinator.domain_counts(0), (0, 0));
    assert_eq!(coordinator.domain_counts(1), (1, 0));
    assert_eq!(sim_ctr.last_domain(), Some(1));
    assert_eq!(flb.stats.tid_map.get(&spill_tid).map(|v| *v), Some(1));
    assert_eq!(flb.preallocated_domains.cgroup_domain(sim_ctr.cgroup_id()), Some(1));

    // Clean up and verify 0 leaks
    tracker.cgroup_released_for_function(sim_ctr.cgroup_id(), &spill_tid, func_reg.clone());
    drop(lifecycle);
    drop(spill_permit);
    drop(blocker_permit);
    assert_eq!(coordinator.domain_counts(1), (0, 0));
}

/// 5. No HOL blocking: Verify a deferred cold boot (`P_i(d*) > B_i`) does not block
/// a `WarmIdle` invocation behind it in `pricing_queue`.
#[iluvatar_library::sim_test]
async fn test_no_hol_blocking_when_cold_boot_is_deferred() {
    let (_log, _cfg, cm, _invoker, reg_svc, cmap, _gpu) = build_test_services(None, None, None).await;

    let cold_reg = reg_svc
        .register(
            RegisterRequest {
                function_name: "cold_short".to_string(),
                function_version: "1".to_string(),
                cpus: 1,
                memory: 128,
                parallel_invokes: 1,
                image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
                transaction_id: "reg-cold".to_string(),
                compute: Compute::CPU.bits(),
                isolate: Isolation::CONTAINERD.bits(),
                ..Default::default()
            },
            &TEST_TID,
        )
        .await
        .unwrap();
    let warm_reg = reg_svc
        .register(
            RegisterRequest {
                function_name: "warm_long".to_string(),
                function_version: "1".to_string(),
                cpus: 1,
                memory: 128,
                parallel_invokes: 1,
                image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
                transaction_id: "reg-warm".to_string(),
                compute: Compute::CPU.bits(),
                isolate: Isolation::CONTAINERD.bits(),
                ..Default::default()
            },
            &TEST_TID,
        )
        .await
        .unwrap();

    // `cold_short` has HIGHER w_0 (`s_i = 10 ms`) than `warm_long` (`s_i = 200 ms`).
    cmap.update(&cold_reg.fqdn, Chars::CpuWarmTime, 0.010);
    cmap.update(&cold_reg.fqdn, Chars::CpuColdTime, 1.000);
    cmap.update(&warm_reg.fqdn, Chars::CpuWarmTime, 0.200);

    // Prewarm `warm_long` so an idle container exists in `cm`, while `cold_short` has NO idle container
    // (only an in-flight boot, so scale-out cold boot faces congestion).
    cm.prewarm(&warm_reg, &TEST_TID, Compute::CPU).await.unwrap();

    let cfg = PostedPriceConfig {
        welfare_beta: Some(0.5),
        cores_per_domain: Some(2),
        ..Default::default()
    };
    let est = Box::new(UnitFootprintEstimator::new(2, 2));
    let coordinator = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap,
        Some(cm.clone()),
        2,
        est,
    );

    // Saturate both 2-core domains (`effective_rho >= 1.0` -> `P_i(d*) > B_i` for cold boot!)
    // and record 1 active boot for `cold_reg` so it is a scale-out cold boot.
    coordinator.on_dispatch(0, "busy", 1.0);
    coordinator.on_dispatch(0, "busy", 1.0);
    coordinator.on_dispatch(1, "busy", 1.0);
    coordinator.on_dispatch(1, "busy", 1.0);
    coordinator.on_boot_start(0, &cold_reg.fqdn);

    let (_, cold_snap) = coordinator.rank_domains_for_dispatch(&cold_reg.fqdn);
    assert!(cold_snap.is_cold);
    assert!(!cold_snap.cold_admitted, "cold boot must be deferred when P_i(d*) > B_i");

    let cold_item = Arc::new(EnqueuedInvocation::new(
        cold_reg.clone(),
        "{}".to_string(),
        "tid-cold-deferred".to_string(),
        OffsetDateTime::now_utc(),
        1.0,
        0.0,
    ));
    let warm_item = Arc::new(EnqueuedInvocation::new(
        warm_reg.clone(),
        "{}".to_string(),
        "tid-warm-ready".to_string(),
        OffsetDateTime::now_utc(),
        0.2,
        0.0,
    ));

    // Enqueue higher-w_0 `cold_item` FIRST, then `warm_item` behind it.
    coordinator.add_item_to_queue(&cold_item, None).unwrap();
    coordinator.add_item_to_queue(&warm_item, None).unwrap();
    assert_eq!(coordinator.queue_len(), 2);

    // `WarmIdle` item MUST step past the deferred cold item with zero HOL blocking!
    let first_popped = coordinator.pop_dispatchable().expect("warm item must be dispatchable");
    assert_eq!(first_popped.tid, "tid-warm-ready");

    // Now only the deferred cold item remains -> `pop_dispatchable` returns `None` (no spin-blocking!).
    assert!(coordinator.peek_queue().is_none());
    assert!(coordinator.pop_dispatchable().is_none());
    assert_eq!(coordinator.queue_len(), 1);

    // Once domain load drains, `P_i(d*) <= B_i` holds and the deferred cold item dispatches cleanly!
    coordinator.on_boot_end(0, &cold_reg.fqdn);
    coordinator.on_complete(0, "busy", 1.0);
    coordinator.on_complete(0, "busy", 1.0);
    coordinator.on_complete(1, "busy", 1.0);
    coordinator.on_complete(1, "busy", 1.0);

    let second_popped = coordinator
        .pop_dispatchable()
        .expect("cold item must become dispatchable once load clears");
    assert_eq!(second_popped.tid, "tid-cold-deferred");
    assert_eq!(coordinator.queue_len(), 0);
}

/// 6. Leak-free lifecycle: Verify in-flight counts, boot counts, and active weights return to `0`
/// across normal completions, cold-boot completions, and early-abort Drop paths.
#[test]
fn test_leak_free_lifecycle_across_normal_and_error_paths() {
    let cmap = worker_char_map();
    cmap.update("fn.alpha", Chars::CpuWarmTime, 0.080);
    let cfg = PostedPriceConfig {
        welfare_beta: Some(0.5),
        cores_per_domain: Some(4),
        ..Default::default()
    };
    let coordinator = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap,
        None,
        4,
        Box::new(UnitFootprintEstimator::new(4, 4)),
    );

    // Path 1: Cold dispatch -> boot finishes -> execution finishes normally
    let item1 = mock_invocation("fn.alpha", "tid-1");
    coordinator.add_item_to_queue(&item1, None).unwrap();
    let popped1 = coordinator.pop_dispatchable().unwrap();
    let (_, snap1) = coordinator.rank_for_item(&popped1);
    let mut lc1 = coordinator.begin_dispatch(&popped1.tid, "fn.alpha", 2, snap1.as_ref(), None);
    assert_eq!(coordinator.domain_counts(2), (1, 1));
    assert!(coordinator.predict_domain(2).effective_rho > 0.0);
    assert!(coordinator.predict_domain(2).active_weight > 0.0);

    // Boot completes before invoke finishes
    lc1.boot_guard.take().unwrap().finish_now();
    assert_eq!(coordinator.domain_counts(2), (1, 0));

    // Execution finishes and updates EWMA substantially; weight must STILL return to exact 0.0
    lc1.dispatch_guard.complete_now();
    coordinator.record_execution_observation(2, "fn.alpha", 950.0, true, Some(120.0));
    drop(lc1);
    assert_eq!(coordinator.domain_counts(2), (0, 0));
    assert_eq!(coordinator.predict_domain(2).effective_rho, 0.0);
    assert_eq!(coordinator.predict_domain(2).active_weight, 0.0);

    // Path 2: Early error / abort drops `PostedPriceInvocationLifecycle` without explicit finish calls
    let (_, snap2) = coordinator.rank_domains_with_state("fn.alpha", false, &[], 1);
    {
        let _lc_abort = coordinator.begin_dispatch(&"tid-abort".to_string(), "fn.alpha", 3, Some(&snap2), None);
        assert_eq!(coordinator.domain_counts(3), (1, 1));
    } // `_lc_abort` dropped here as on container/cgroup error!

    for d in 0..4 {
        assert_eq!(coordinator.domain_counts(d), (0, 0));
        let est = coordinator.predict_domain(d);
        assert_eq!(est.effective_rho, 0.0);
        assert_eq!(est.active_weight, 0.0);
    }
}

/// 7. FIFO tie-breaking, `aging_grace_sec` deadband, and strict un-aged downstream `w_0`:
/// - Within `aging_grace_sec` (default 1.5 s), `w_eff == w_0` bit-identically and equal-`w_eff`
///   clones (`pyaes`, `pyaes-1`, `pyaes-2`, `pyaes-3`) pop strictly in `enqueue_ts` FIFO order.
/// - Beyond `aging_grace_sec`, `w_eff = w_0 * (1.0 + aging_alpha * (wait_s - aging_grace_sec))`
///   promotes starved batch invocations while keeping `hello` isolated.
/// - Downstream consumers (`rank_for_item`, `begin_dispatch`, `DomainLoadEstimator`, and BPF
///   assignment) always receive the un-aged base `w_0 = s_i.powf(-beta)`.
#[test]
fn test_fifo_tie_breaking_deadband_aging_and_unaged_downstream_w0() {
    let cmap = worker_char_map();
    for fqdn in ["pyaes", "pyaes-1", "pyaes-2", "pyaes-3"] {
        cmap.update(fqdn, Chars::CpuWarmTime, 0.480);
    }
    cmap.update("bfs", Chars::CpuWarmTime, 0.060);
    cmap.update("hello", Chars::CpuWarmTime, 0.001);

    // Part A: Default `aging_grace_sec = 1.5`, `aging_alpha = 1.0`.
    // Verify `w_eff == w_0` bit-identically up to 1.5 s, and exact formula above 1.5 s.
    let default_coord = PostedPriceCoordinator::with_estimator(
        None,
        FineLoadBalancingWeak::new(),
        cmap.clone(),
        None,
        4,
        Box::new(UnitFootprintEstimator::new(4, 6)),
    );
    let (_, base_w0) = default_coord.priority_weight("pyaes-3");
    assert_eq!(default_coord.effective_queue_priority("pyaes-3", 0.0).to_bits(), base_w0.to_bits());
    assert_eq!(default_coord.effective_queue_priority("pyaes-3", 0.75).to_bits(), base_w0.to_bits());
    assert_eq!(default_coord.effective_queue_priority("pyaes-3", 1.50).to_bits(), base_w0.to_bits());
    let expected_at_3p5 = base_w0 * (1.0 + 1.0 * (3.5 - 1.5));
    assert!((default_coord.effective_queue_priority("pyaes-3", 3.50) - expected_at_3p5).abs() < 1e-15);

    // Within the 1.5 s deadband, equal-weight clones pop strictly in `enqueue_ts` FIFO order,
    // including across `requeue_front`.
    let arrival_order = ["pyaes-3", "pyaes-1", "pyaes", "pyaes-2"];
    for (idx, fqdn) in arrival_order.iter().enumerate() {
        let item = mock_invocation(fqdn, &format!("tid-{idx}-{fqdn}"));
        default_coord.add_item_to_queue(&item, None).unwrap();
    }
    let first = default_coord.pop_dispatchable().unwrap();
    assert_eq!(first.registration.fqdn, "pyaes-3");
    default_coord.requeue_front(&first);

    let popped_fqdns: Vec<String> = (0..4)
        .map(|_| default_coord.pop_dispatchable().unwrap().registration.fqdn.clone())
        .collect();
    assert_eq!(popped_fqdns, arrival_order);

    // Part B: Beyond `aging_grace_sec`, starved `pyaes-3` overtakes fresh `bfs` while `hello`
    // stays ahead of both, AND downstream `rank_for_item` + `begin_dispatch` receive un-aged `w_0`!
    let cfg_deadband = PostedPriceConfig {
        welfare_beta: Some(0.5),
        cores_per_domain: Some(6),
        aging_grace_sec: Some(0.020), // 20 ms grace for fast unit test
        aging_alpha: Some(150.0),     // 30 ms past grace -> 1 + 150 * 0.03 = 5.5x (> bfs/pyaes = 2.83x, << hello/pyaes = 21.9x)
        ..Default::default()
    };
    let aged_coord = PostedPriceCoordinator::with_estimator(
        Some(&cfg_deadband),
        FineLoadBalancingWeak::new(),
        cmap,
        None,
        4,
        Box::new(UnitFootprintEstimator::new(4, 6)),
    );

    let old_pyaes = mock_invocation("pyaes-3", "tid-old-pyaes");
    aged_coord.add_item_to_queue(&old_pyaes, None).unwrap();

    // Before deadband expires (0 ms), fresh `bfs` still beats `pyaes-3`.
    let early_bfs = mock_invocation("bfs", "tid-early-bfs");
    aged_coord.add_item_to_queue(&early_bfs, None).unwrap();
    assert_eq!(aged_coord.pop_dispatchable().unwrap().tid, "tid-early-bfs");

    // Sleep past 20 ms grace + 30 ms effective wait (55 ms total).
    std::thread::sleep(std::time::Duration::from_millis(55));
    let fresh_bfs = mock_invocation("bfs", "tid-fresh-bfs");
    let fresh_hello = mock_invocation("hello", "tid-fresh-hello");
    aged_coord.add_item_to_queue(&fresh_bfs, None).unwrap();
    aged_coord.add_item_to_queue(&fresh_hello, None).unwrap();

    // 1st pop: `hello` (`w_0 = 1.0`), 2nd pop: aged `pyaes-3`, 3rd pop: fresh `bfs`.
    assert_eq!(aged_coord.pop_dispatchable().unwrap().tid, "tid-fresh-hello");
    let popped_aged_pyaes = aged_coord.pop_dispatchable().unwrap();
    assert_eq!(popped_aged_pyaes.tid, "tid-old-pyaes");
    assert_eq!(aged_coord.pop_dispatchable().unwrap().tid, "tid-fresh-bfs");

    // Verify downstream `rank_for_item`, `begin_dispatch`, and `DomainLoadEstimator` use UN-AGED `w_0`!
    let (_, snap_opt) = aged_coord.rank_for_item(&popped_aged_pyaes);
    let snap = snap_opt.expect("priced snapshot");
    assert_eq!(snap.w_0.to_bits(), base_w0.to_bits(), "snapshot w_0 must be un-aged");
    assert!((snap.benefit_b_i - base_w0 * 480.0).abs() < 1e-12);
    for cand in &snap.candidates {
        assert_eq!(cand.w_0.to_bits(), base_w0.to_bits(), "candidate w_0 must be un-aged");
    }
    let lifecycle = aged_coord.begin_dispatch(&popped_aged_pyaes.tid, "pyaes-3", 0, Some(&snap), None);
    assert_eq!(lifecycle.w_0.to_bits(), base_w0.to_bits(), "lifecycle w_0 for BPF must be un-aged");
    assert_eq!(
        aged_coord.predict_domain(0).active_weight.to_bits(),
        base_w0.to_bits(),
        "DomainLoadEstimator W_d must track un-aged w_0"
    );
    drop(lifecycle);
    assert_eq!(aged_coord.predict_domain(0).active_weight, 0.0);
}

/// 8. L1/L2 No float drift: Dispatch 500 mixed invocations while updating `u_hat[fqdn]` mid-flight;
/// assert `L_l1[d] == 0.0` (within `1e-9`) after all complete (and verify partial sum accuracy mid-flight).
#[test]
fn test_l1_l2_no_float_drift_across_500_mixed_invocations() {
    let mut l1 = UtilWeightedEstimator::from_config(4, 6, None);
    let mut l2 = PeltBiasObserverEstimator::from_config(4, 6, None);
    let fqdns = ["hello", "bfs", "gzip", "pyaes"];

    // Hold a long-running anchor invocation on each domain while 500 mixed tasks start/complete
    // and mutate `u_hat[fqdn]` mid-flight, testing both non-zero intermediate sums and final 0.0!
    let anchor_l1: Vec<f64> = (0..4).map(|d| l1.on_dispatch(d, "pyaes", 0.045)).collect();
    let anchor_l2: Vec<f64> = (0..4).map(|d| l2.on_dispatch(d, "pyaes", 0.045)).collect();

    let mut active_l1: Vec<(usize, &'static str, f64, f64)> = Vec::new();
    let mut active_l2: Vec<(usize, &'static str, f64, f64)> = Vec::new();

    for i in 0..500usize {
        let d = i % 4;
        let fqdn = fqdns[(i * 7 + 3) % fqdns.len()];
        let w = 0.05 + 0.03 * ((i % 5) as f64);
        let fp1 = l1.on_dispatch(d, fqdn, w);
        let fp2 = l2.on_dispatch(d, fqdn, w);
        active_l1.push((d, fqdn, w, fp1));
        active_l2.push((d, fqdn, w, fp2));

        // Mutate `u_hat[fqdn]` mid-flight with varying utilization samples!
        let util = 0.25 + 0.17 * (((i * 13) % 5) as f64);
        l1.on_util_sample(d, fqdn, util, 0.20);
        l2.on_util_sample(d, fqdn, util, 0.20);

        // Complete half of the backlog out-of-order (popping from middle) while others stay in flight.
        if active_l1.len() >= 9 {
            let mid = active_l1.len() / 2;
            let (cd, cf, cw, cfp1) = active_l1.remove(mid);
            let (_, _, _, cfp2) = active_l2.remove(mid);
            l1.on_complete(cd, cf, cw, cfp1);
            l2.on_complete(cd, cf, cw, cfp2);
        }
    }

    // Drain remaining 500 invocations while anchors are STILL in flight (`in_flight_count == 1`),
    // proving intermediate sum matches exact `anchor_fp` without relying solely on zero-snap!
    for (cd, cf, cw, cfp1) in active_l1 {
        l1.on_complete(cd, cf, cw, cfp1);
    }
    for (cd, cf, cw, cfp2) in active_l2 {
        l2.on_complete(cd, cf, cw, cfp2);
    }
    for d in 0..4 {
        assert!(
            (l1.l1_load_cores(d) - anchor_l1[d]).abs() < 1e-9,
            "L1 mid-flight drift on d{d}: {} vs {}",
            l1.l1_load_cores(d),
            anchor_l1[d]
        );
        assert!(
            (l2.l1_load_cores(d) - anchor_l2[d]).abs() < 1e-9,
            "L2 mid-flight drift on d{d}: {} vs {}",
            l2.l1_load_cores(d),
            anchor_l2[d]
        );
        l1.on_complete(d, "pyaes", 0.045, anchor_l1[d]);
        l2.on_complete(d, "pyaes", 0.045, anchor_l2[d]);
        assert!(l1.l1_load_cores(d).abs() < 1e-9);
        assert!(l2.l1_load_cores(d).abs() < 1e-9);
        assert_eq!(l1.predict(d).active_weight, 0.0);
        assert_eq!(l2.predict(d).active_weight, 0.0);
    }

    // Also verify config deserialization of `"unit"` | `"util_weighted"` | `"pelt_observer"`.
    let parsed: PostedPriceConfig = serde_json::from_str(
        r#"{"load_estimator":"pelt_observer","util_ewma_alpha":0.2,"util_low_contention_rho":0.65,"pelt_half_life_ms":32.0,"observer_kp":0.5,"observer_kb":0.2}"#,
    )
    .unwrap();
    assert_eq!(parsed.load_estimator, Some(LoadEstimatorKind::PeltObserver));
}

/// 9. Contention-shrinkage immunity (L1) + `max(1.0, u_hat)` floor:
/// Train `u_hat["bfs"]` to `~0.86` at `rho = 0.3`, then feed 50 samples of `0.65` at `rho = 0.9`;
/// assert `raw_u_hat("bfs")` remains `>= 0.82`, `estimated_footprint("bfs") == 1.0`, and
/// `u_hat > 1.0` (`1.8`) returns `1.8`.
#[test]
fn test_l1_contention_shrinkage_immunity() {
    let mut l1 = UtilWeightedEstimator::from_config(4, 6, None);
    l1.on_pelt_sample(0, 0.30, 1.8);
    for _ in 0..20 {
        l1.on_util_sample(0, "bfs", 0.86, 0.30);
        l1.on_util_sample(0, "mt", 1.80, 0.30);
    }
    assert!((l1.raw_u_hat("bfs") - 0.86).abs() < 1e-6);
    assert!((l1.estimated_footprint("bfs") - 1.0).abs() < 1e-12);
    assert!((l1.estimated_footprint("mt") - 1.80).abs() < 1e-6);

    // Now saturate domain (`rho = 0.90`) and feed 50 shrunk samples (`0.65`).
    for _ in 0..50 {
        l1.on_util_sample(0, "bfs", 0.65, 0.90);
    }
    assert!(
        l1.raw_u_hat("bfs") >= 0.82,
        "u_hat[bfs] shrunk under contention to {}",
        l1.raw_u_hat("bfs")
    );

    // Also verify high `pelt_util` (`0.80 >= 0.65`) blocks downward shrinkage even if `domain_rho` was low.
    l1.on_pelt_sample(0, 0.80, 5.0);
    for _ in 0..50 {
        l1.on_util_sample(0, "bfs", 0.60, 0.20);
    }
    assert!(l1.raw_u_hat("bfs") >= 0.82);
}

/// 10. Zero double-counting on burst (L2): Dispatch 4 tasks (`u = 1.0`) at `t = 0`,
/// advance simulated time in 10 ms steps feeding exact 32 ms exponential PELT ground truth
/// `y_d(t) = 4 * (1 - 2^(-t/32))`; assert `effective_rho` jumps immediately at `t = 0`
/// and `|b_hat[d]| < 0.05` throughout the ramp.
#[test]
fn test_l2_zero_double_counting_on_burst() {
    let t0 = std::time::Instant::now();
    let mut l2 = PeltBiasObserverEstimator::from_config_at(4, 6, None, t0);

    for _ in 0..4 {
        l2.on_dispatch_at(0, "pyaes", 0.045, t0);
    }
    // Immediate jump at t = 0 (`4.0 / 6.0`, zero PELT lag!).
    let rho_t0 = l2.predict(0).effective_rho;
    assert!((rho_t0 - 4.0 / 6.0).abs() < 1e-12);
    assert!(l2.bias_cores(0).abs() < 0.05);

    for step in 1..=30 {
        let t_ms = (step * 10) as f64;
        let now = t0 + std::time::Duration::from_millis((step * 10) as u64);
        let y_d = 4.0 * (1.0 - 2.0_f64.powf(-t_ms / 32.0));
        let pelt_util = y_d / 6.0;
        l2.on_pelt_sample_at(0, pelt_util, y_d, now);
        assert!(
            l2.bias_cores(0).abs() < 0.05,
            "step {step} (t={t_ms}ms): |b_hat| = {} >= 0.05",
            l2.bias_cores(0).abs()
        );
        assert!((l2.predict(0).effective_rho - 4.0 / 6.0).abs() < 0.01);
    }
}

/// 11. External load discovery (L2): With 0 Ilúvatar tasks in flight and `y_d = 3.0` cores
/// every 10 ms, assert `b_hat[d]` converges to `~3.0` cores and decays back toward `0` when `y_d` returns to `0`.
#[test]
fn test_l2_external_load_discovery_and_idle_decay() {
    let t0 = std::time::Instant::now();
    let mut l2 = PeltBiasObserverEstimator::from_config_at(4, 6, None, t0);

    let mut now = t0;
    for _ in 0..80 {
        now += std::time::Duration::from_millis(10);
        l2.on_pelt_sample_at(0, 3.0 / 6.0, 3.0, now);
    }
    assert!(
        (l2.bias_cores(0) - 3.0).abs() < 0.05,
        "b_hat failed to converge to ~3.0 cores: {}",
        l2.bias_cores(0)
    );
    assert!((l2.predict(0).effective_rho - 0.5).abs() < 0.01);

    // External load stops (`y_d = 0.0` with `L_l1 == 0.0`) -> `b_hat` decays back toward `0.0`.
    for _ in 0..60 {
        now += std::time::Duration::from_millis(10);
        l2.on_pelt_sample_at(0, 0.0, 0.0, now);
    }
    assert!(
        l2.bias_cores(0).abs() < 0.02,
        "b_hat failed to decay toward 0 when idle: {}",
        l2.bias_cores(0)
    );
}

/// 12. Over-estimation correction (L2): Dispatch 4 tasks with prior `u = 1.0` while true PELT
/// settles at `1.6` cores (`0.4` per task); assert `b_hat[d]` converges negative to bring `L_hat_d` close to `1.6`.
#[test]
fn test_l2_overestimation_correction() {
    let t0 = std::time::Instant::now();
    let mut l2 = PeltBiasObserverEstimator::from_config_at(4, 6, None, t0);

    for _ in 0..4 {
        let fp = l2.on_dispatch_at(0, "hello-heavy-prior", 1.0, t0);
        assert_eq!(fp, 1.0);
    }
    assert!((l2.l1_load_cores(0) - 4.0).abs() < 1e-12);

    let mut now = t0;
    for step in 1..=80 {
        let t_ms = (step * 10) as f64;
        now += std::time::Duration::from_millis(10);
        let y_d = 1.6 * (1.0 - 2.0_f64.powf(-t_ms / 32.0));
        l2.on_pelt_sample_at(0, y_d / 6.0, 1.6, now);
    }

    assert!(
        l2.bias_cores(0) < -2.2,
        "b_hat should converge near -2.4 cores, got {}",
        l2.bias_cores(0)
    );
    assert!(
        (l2.l_hat_cores(0) - 1.6).abs() < 0.10,
        "L_hat_d should converge near 1.6 cores, got {}",
        l2.l_hat_cores(0)
    );
}

/// 13. Express ranking demotes `in_flight_count >= cores_per_domain` domains without dropping them:
/// - Under L1 (`u_hat < 1.0`), a full `6/6` warm domain with `effective_rho < 1.0` is demoted
///   behind `< 6/6` domains (warm preferred), while keeping all 4 domains in the slice.
/// - When all 4 domains hit `6/6` simultaneously, returns all 4 sorted by ascending `effective_rho`.
#[test]
fn test_express_ranking_demotes_full_domains_and_preserves_full_slice() {
    let cmap = worker_char_map();
    let cfg = PostedPriceConfig {
        welfare_beta: Some(0.5),
        cores_per_domain: Some(6),
        load_estimator: Some(LoadEstimatorKind::UtilWeighted),
        ..Default::default()
    };
    let mut l1 = UtilWeightedEstimator::from_config(4, 6, Some(&cfg));
    l1.on_util_sample(0, "light", 0.40, 0.10); // max(1.0, 0.40) = 1.0
    l1.on_util_sample(0, "heavy", 1.10, 0.10); // max(1.0, 1.10) = 1.10

    // d0: 6/6 full (`6 * 1.0 / 6 = 1.0`), warm
    for _ in 0..6 {
        l1.on_dispatch(0, "light", 1.0);
    }
    // d1: 5/6 spare slot (`5 * 1.10 / 6 = 0.917 < 1.0`), warm
    for _ in 0..5 {
        l1.on_dispatch(1, "heavy", 0.1);
    }
    // d2: 1/6 spare slot (`1 * 1.0 / 6 = 0.167`), non-warm
    l1.on_dispatch(2, "light", 1.0);
    // d3: 6/6 full (`6 * 1.10 / 6 = 1.10`), non-warm
    for _ in 0..6 {
        l1.on_dispatch(3, "heavy", 0.1);
    }

    let coord = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap.clone(),
        None,
        4,
        Box::new(l1),
    );
    let ranked = coord.rank_domains_express_with_warm(&[0, 1]);
    // d1 (<6/6 & warm) -> d2 (<6/6 & non-warm) -> d0 (6/6, rho=1.00) -> d3 (6/6, rho=1.10)
    assert_eq!(ranked, vec![1, 2, 0, 3]);

    // Now saturate ALL 4 domains to `6/6` with distinct `effective_rho`:
    // d0=1.80, d1=1.00, d2=2.20, d3=1.40 -> must return all 4 in `[1, 3, 0, 2]` order!
    let mut l1_full = UtilWeightedEstimator::from_config(4, 6, Some(&cfg));
    for (name, u) in [("f0", 1.80), ("f1", 0.40), ("f2", 2.20), ("f3", 1.40)] {
        l1_full.on_util_sample(0, name, u, 0.10);
    }
    for (d, name) in [(0, "f0"), (1, "f1"), (2, "f2"), (3, "f3")] {
        for _ in 0..6 {
            l1_full.on_dispatch(d, name, 0.5);
        }
    }
    let full_coord = PostedPriceCoordinator::with_estimator(
        Some(&cfg),
        FineLoadBalancingWeak::new(),
        cmap,
        None,
        4,
        Box::new(l1_full),
    );
    let ranked_all_full = full_coord.rank_domains_express_with_warm(&[0, 2]);
    assert_eq!(ranked_all_full.len(), 4);
    assert_eq!(ranked_all_full, vec![1, 3, 0, 2]);
}

/// 14. L2 pre-cgroup cold-boot immunity:
/// Verify that pre-cgroup cold-starting invocations (`on_dispatch` + `on_boot_start_with_footprint`)
/// contribute `u_boot` to `L_hat_d` while booting WITHOUT driving `b_hat[d]` negative when
/// domain cgroup PELT only reflects already-running warm containers.
#[test]
fn test_l2_cold_boot_does_not_drive_negative_bias() {
    let t0 = std::time::Instant::now();
    let mut l2 = PeltBiasObserverEstimator::from_config_at(4, 6, None, t0);
    l2.on_util_sample(0, "bfs", 0.85, 0.20); // max(1.0, 0.85) = 1.0

    // 3 warm `bfs` running inside d0's cgroup (`3 * 1.0 = 3.0` cores).
    for _ in 0..3 {
        l2.on_dispatch_at(0, "bfs", 0.14, t0);
    }
    // 2 cold `pyaes` dispatched onto d0 that are still inside pre-cgroup Docker creation (`u_boot = 1.0` each).
    let cold_fp1 = l2.on_dispatch_at(0, "pyaes", 0.045, t0);
    l2.on_boot_start_with_footprint_at(0, "pyaes", cold_fp1, t0);
    let cold_fp2 = l2.on_dispatch_at(0, "pyaes", 0.045, t0);
    l2.on_boot_start_with_footprint_at(0, "pyaes", cold_fp2, t0);

    // Instantaneous L_hat_d counts 3.0 (warm) + 2.0 (boots) = 5.0 cores (no 2x double-count).
    assert!((l2.l_hat_cores(0) - 5.0).abs() < 1e-9);

    // Over a 500 ms Docker creation window, domain cgroup PELT sees ONLY the 3 warm `bfs` (`3.0` cores).
    let mut now = t0;
    for step in 1..=50 {
        let t_ms = (step * 10) as f64;
        now += std::time::Duration::from_millis(10);
        let y_d = 3.0 * (1.0 - 2.0_f64.powf(-t_ms / 32.0));
        l2.on_pelt_sample_at(0, y_d / 6.0, 3.0, now);
        assert!(
            l2.bias_cores(0).abs() < 0.05,
            "cold boot drove false negative bias at t={t_ms}ms: b_hat={}",
            l2.bias_cores(0)
        );
    }

    // Both cold boots finish and enter the cgroup -> `L_hat_d` transitions cleanly to `3.0 + 2.0 = 5.0`
    // and PELT ramps to `5.0` with `|b_hat| < 0.05`.
    l2.on_boot_end_with_footprint_at(0, "pyaes", cold_fp1, now);
    l2.on_boot_end_with_footprint_at(0, "pyaes", cold_fp2, now);
    for step in 1..=30 {
        let dt_ms = (step * 10) as f64;
        now += std::time::Duration::from_millis(10);
        let y_d = 3.0 + 2.0 * (1.0 - 2.0_f64.powf(-dt_ms / 32.0));
        l2.on_pelt_sample_at(0, y_d / 6.0, 5.0, now);
        assert!(l2.bias_cores(0).abs() < 0.05);
    }
}

/// 15. Topology portability across 1-domain, 6-domain, and heterogeneous uneven-core domains:
/// Verify zero hardcoded 4-domain or uniform-core assumptions in estimators, express ranking,
/// or priced ranking.
#[test]
fn test_portable_domain_counts_and_heterogeneous_cores() {
    let cmap = iluvatar_library::char_map::worker_char_map();
    cmap.update("bfs", Chars::CpuWarmTime, 0.060);
    cmap.update("hello", Chars::CpuWarmTime, 0.001);

    // Case A: Single-domain 16-core host (no FineSched).
    let single = PostedPriceCoordinator::new_with_fallback_cores(
        None,
        FineLoadBalancingWeak::new(),
        cmap.clone(),
        None,
        16,
    );
    assert_eq!(single.num_domains(), 1);
    assert_eq!(single.domain_cores(0), 16);
    assert_eq!(single.rank_domains_express_with_warm(&[0]), vec![0]);
    let (ranked_1, snap_1) = single.rank_domains_with_state("bfs", true, &[0], 1);
    assert_eq!(ranked_1, vec![0]);
    assert_eq!(snap_1.candidates.len(), 1);

    // Case B: 6-domain × 8-core host (48 logical CPUs).
    let cfg_l1 = PostedPriceConfig {
        load_estimator: Some(LoadEstimatorKind::UtilWeighted),
        ..Default::default()
    };
    let est_6 = Box::new(UtilWeightedEstimator::from_config_heterogeneous(
        vec![8; 6],
        Some(&cfg_l1),
    ));
    let six = PostedPriceCoordinator::with_domain_cores_and_estimator(
        Some(&cfg_l1),
        FineLoadBalancingWeak::new(),
        cmap.clone(),
        None,
        vec![8; 6],
        est_6,
    );
    assert_eq!(six.num_domains(), 6);
    for d in [0, 1, 2, 3, 5] {
        six.on_dispatch(d, "bfs", 0.13);
    }
    let (ranked_6, snap_6) = six.rank_domains_with_state("bfs", true, &[0, 4], 2);
    assert_eq!(ranked_6.len(), 6);
    assert_eq!(snap_6.candidates.len(), 6);
    assert_eq!(ranked_6[0], 4, "idle warm domain d4 out of 6 must rank first");

    // Case C: Heterogeneous 3-domain layout `[8, 8, 4]` (e.g., 20-core uneven split).
    // With 4 active tasks on d0 (`4/8 = 0.50`, slots free) and 4 active tasks on d2 (`4/4 = 1.00`, full),
    // both express and priced ranking must prefer d0 over full d2 and keep all 3 domains in slice.
    let mut het_l1 = UtilWeightedEstimator::from_config_heterogeneous(vec![8, 8, 4], Some(&cfg_l1));
    for _ in 0..4 {
        het_l1.on_dispatch(0, "bfs", 0.13);
        het_l1.on_dispatch(2, "bfs", 0.13);
    }
    for _ in 0..8 {
        het_l1.on_dispatch(1, "bfs", 0.13);
    }
    let het = PostedPriceCoordinator::with_domain_cores_and_estimator(
        Some(&cfg_l1),
        FineLoadBalancingWeak::new(),
        cmap,
        None,
        vec![8, 8, 4],
        Box::new(het_l1),
    );
    assert_eq!(het.num_domains(), 3);
    assert!((het.predict_domain(0).effective_rho - 0.50).abs() < 1e-12);
    assert!((het.predict_domain(2).effective_rho - 1.00).abs() < 1e-12);

    let exp_het = het.rank_domains_express_with_warm(&[1, 2]);
    assert_eq!(exp_het, vec![0, 2, 1], "d0 (4/8 < 8) must precede full d2 (4/4, W=0.52) and d1 (8/8, W=1.04)");
    let (priced_het, _) = het.rank_domains_with_state("bfs", true, &[2], 2);
    assert_eq!(priced_het[0], 0, "d0 with free slots must rank ahead of full 4/4 warm d2");
    assert_eq!(priced_het.len(), 3);
}
