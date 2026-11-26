#[macro_use]
pub mod utils;

use crate::utils::build_test_services;
use iluvatar_finesched::SchedGroup;
use iluvatar_finesched::SchedGroupID;
use iluvatar_library::char_map::Chars;
use iluvatar_library::transaction::{gen_tid, TEST_TID};
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::services::resources::fineloadbalancing::{BuildFineLoadBalancing, FineLoadBalancing};
use iluvatar_worker_library::worker_api::worker_config::FineLoadBalancingConfig;
use std::sync::Arc;

fn cpu_reg() -> RegisterRequest {
    RegisterRequest {
        function_name: gen_tid(),
        function_version: "test".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
        transaction_id: "testTID".to_string(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        ..Default::default()
    }
}

#[cfg(test)]
mod fineloadbalancing_iat_chbl_tests {
    use super::*;

    #[iluvatar_library::sim_test]
    async fn hashing_test() {
        let lbpolicy = &"iat_consistent_hashing".to_string();
        let value_type = Chars::IAT;
        let bucket_size_ms = 4000;
        let values_secs = vec![1.0, 5.0, 3.0, 4.0, 7.0, 8.0, 10.0];
        let _expected_buckets = vec![0, 1, 0, 1, 1, 2, 2];
        let expected_domains = vec![0, 1, 0, 1, 1, 2, 2];

        let (_log, _cfg, _cm, _invoker, reg, cmap, _gpu) = build_test_services(None, None, None).await;

        let scheduling_group = SchedGroup {
            cores: vec![1],
            ..Default::default()
        };
        let mut config = FineLoadBalancingConfig::default();
        config.dispatchpolicy = lbpolicy.clone();
        config.testing = 1;
        config.iat_bucket_size = bucket_size_ms;
        config.preallocated_groups.groups = vec![
            scheduling_group.clone(),
            scheduling_group.clone(),
            scheduling_group.clone(),
        ];

        let loadbalancing = FineLoadBalancing::build_arc(Arc::new(config), cmap.clone());

        let mut func_regs: Vec<Arc<RegisteredFunction>> = vec![];
        for value in values_secs.iter() {
            let func_reg = reg
                .register(cpu_reg(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            cmap.update(&func_reg.fqdn, value_type, *value);
            func_regs.insert(func_regs.len(), func_reg.clone());
        }

        for (func_reg, expected_domain_id) in func_regs.iter().zip(expected_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }
    }
}

#[cfg(test)]
mod fineloadbalancing_chlb_rebalance_tests {
    use super::*;

    async fn rebalance_test_for(
        lbpolicy: &String,
        value_type: Chars,
        values: Vec<f64>,
        first_time_domains: Vec<SchedGroupID>,
        rebalanced_expected_domains: Vec<SchedGroupID>,
    ) {
        let (_log, _cfg, _cm, _invoker, reg, cmap, _gpu) = build_test_services(None, None, None).await;

        let scheduling_group = SchedGroup {
            cores: vec![1],
            ..Default::default()
        };
        let mut config = FineLoadBalancingConfig::default();
        config.dispatchpolicy = lbpolicy.clone();
        config.testing = 1;
        config.preallocated_groups.groups = vec![
            scheduling_group.clone(),
            scheduling_group.clone(),
            scheduling_group.clone(),
        ];

        let loadbalancing = FineLoadBalancing::build_arc(Arc::new(config), cmap.clone());

        let mut func_regs: Vec<Arc<RegisteredFunction>> = vec![];
        for value in values.iter() {
            let func_reg = reg
                .register(cpu_reg(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            cmap.update(&func_reg.fqdn, value_type, *value);
            func_regs.insert(func_regs.len(), func_reg.clone());
        }

        // first time assignment
        for (func_reg, expected_domain_id) in func_regs.iter().zip(first_time_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }

        // rebalanced lookup
        for (func_reg, expected_domain_id) in func_regs.iter().zip(rebalanced_expected_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);
            assert_eq!(domain_id, expected_domain_id);
        }
    }

    #[iluvatar_library::sim_test]
    async fn iat_rebalance_test() {
        rebalance_test_for(
            &"consistent_hashing_IATRebalance".to_string(),
            Chars::IAT,
            /*values=*/ vec![2.0, 1.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            /*first_time_domains=*/ vec![1, 2, 0, 1, 2, 0, 1],
            /*rebalanced_expected_domains=*/ vec![1, 0, 2, 2, 1, 0, 0],
        )
        .await;
    }

    #[iluvatar_library::sim_test]
    async fn cpuutil_rebalance_test() {
        rebalance_test_for(
            &"consistent_hashing_cpuutilRebalance".to_string(),
            Chars::CPUtil,
            /*values=*/ vec![2.0, 1.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            /*first_time_domains=*/ vec![1, 2, 0, 1, 2, 0, 1],
            /*rebalanced_expected_domains=*/ vec![1, 0, 2, 2, 1, 0, 0],
        )
        .await;
    }

    #[iluvatar_library::sim_test]
    async fn iatcpuutil_rebalance_test() {
        let lbpolicy = &"consistent_hashing_iatcpuutilRebalance".to_string();
        let iats = vec![10.0, 20.0, 30.0, 400.0, 500.0, 600.0];
        let cpu_utils = vec![10.0, 1000.0, 200.0, 500.0, 10.0, 20.0];
        // hybrid values [1100.0, 21.0, 155.0, 802.0, 50100.0, 30050.0]
        let first_time_domains = vec![1, 2, 0, 1, 2, 0];
        let rebalanced_expected_domains = vec![2, 0, 1, 2, 0, 1];

        let (_log, _cfg, _cm, _invoker, reg, cmap, _gpu) = build_test_services(None, None, None).await;

        let scheduling_group = SchedGroup {
            cores: vec![1],
            ..Default::default()
        };
        let mut config = FineLoadBalancingConfig::default();
        config.dispatchpolicy = lbpolicy.clone();
        config.testing = 1;
        config.preallocated_groups.groups = vec![
            scheduling_group.clone(),
            scheduling_group.clone(),
            scheduling_group.clone(),
        ];

        let loadbalancing = FineLoadBalancing::build_arc(Arc::new(config), cmap.clone());

        let mut func_regs: Vec<Arc<RegisteredFunction>> = vec![];
        for (iat, cpu_util) in iats.iter().zip(cpu_utils) {
            let func_reg = reg
                .register(cpu_reg(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            cmap.update(&func_reg.fqdn, Chars::CPUtil, cpu_util);
            cmap.update(&func_reg.fqdn, Chars::IAT, *iat);
            func_regs.insert(func_regs.len(), func_reg.clone());
        }

        // first time assignment
        for (func_reg, expected_domain_id) in func_regs.iter().zip(first_time_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }

        // rebalanced lookup
        for (func_reg, expected_domain_id) in func_regs.iter().zip(rebalanced_expected_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }
    }

    #[iluvatar_library::sim_test]
    async fn iatenergy_rebalance_test() {
        let lbpolicy = &"consistent_hashing_iatenergyRebalance".to_string();
        let iats = vec![10.0, 20.0, 30.0, 400.0, 500.0, 600.0];
        let cpu_utils = vec![10.0, 1000.0, 200.0, 500.0, 450.0, 250.0];
        let durs = vec![10.0, 1000.0, 200.0, 500.0, 300.0, 120.0];
        // hybrid values [110000.0, 1000020000.0, 40030000.0, 250400000.0, 135500000.0, 30600000.0]
        // [(110000.0, 0), (30600000.0, 5), (40030000.0, 2), (135500000.0, 4), (250400000.0, 3), (1000020000.0, 1)]

        let first_time_domains = vec![1, 2, 0, 1, 2, 0];
        let rebalanced_expected_domains = vec![0, 0, 2, 1, 2, 1];

        let (_log, _cfg, _cm, _invoker, reg, cmap, _gpu) = build_test_services(None, None, None).await;

        let scheduling_group = SchedGroup {
            cores: vec![1],
            ..Default::default()
        };
        let mut config = FineLoadBalancingConfig::default();
        config.dispatchpolicy = lbpolicy.clone();
        config.testing = 1;
        config.preallocated_groups.groups = vec![
            scheduling_group.clone(),
            scheduling_group.clone(),
            scheduling_group.clone(),
        ];

        let loadbalancing = FineLoadBalancing::build_arc(Arc::new(config), cmap.clone());

        let mut func_regs: Vec<Arc<RegisteredFunction>> = vec![];
        for ((iat, cpu_util), dur) in iats.iter().zip(cpu_utils).zip(durs) {
            let func_reg = reg
                .register(cpu_reg(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            cmap.update(&func_reg.fqdn, Chars::CpuWarmTime, dur);
            cmap.update(&func_reg.fqdn, Chars::CPUtil, cpu_util);
            cmap.update(&func_reg.fqdn, Chars::IAT, *iat);
            func_regs.insert(func_regs.len(), func_reg.clone());
        }

        // first time assignment
        for (func_reg, expected_domain_id) in func_regs.iter().zip(first_time_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }

        // rebalanced lookup
        for (func_reg, expected_domain_id) in func_regs.iter().zip(rebalanced_expected_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }
    }
}

#[cfg(test)]
mod fineloadbalancing_guardrails_tests {
    use super::*;

    async fn test_domains(lbpolicy: &String, durs: Vec<f64>, expected_domains: Vec<SchedGroupID>) {
        let (_log, _cfg, _cm, _invoker, reg, cmap, _gpu) = build_test_services(None, None, None).await;

        let scheduling_group = SchedGroup {
            cores: vec![1],
            ..Default::default()
        };
        let mut config = FineLoadBalancingConfig::default();
        config.dispatchpolicy = lbpolicy.clone();
        config.testing = 1;
        config.preallocated_groups.groups = vec![
            scheduling_group.clone(),
            scheduling_group.clone(),
            scheduling_group.clone(),
        ];
        config.guardrails_log_base_c = 2;
        config.guardrails_tightness = 1;

        let loadbalancing = FineLoadBalancing::build_arc(Arc::new(config), cmap.clone());

        let mut func_regs: Vec<Arc<RegisteredFunction>> = vec![];
        for dur in durs.iter() {
            let func_reg = reg
                .register(cpu_reg(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            cmap.update(&func_reg.fqdn, Chars::CpuWarmTime, *dur);
            func_regs.insert(func_regs.len(), func_reg.clone());
        }

        for (func_reg, expected_domain_id) in func_regs.iter().zip(expected_domains) {
            let domain_id = loadbalancing
                .lbpolicy
                .assign_domain_to_function_request(&TEST_TID, func_reg.clone())
                .unwrap();
            loadbalancing.stats.tid_map.insert(TEST_TID.to_string(), domain_id);
            loadbalancing.lbpolicy.release_domain(&TEST_TID, func_reg.clone());
            loadbalancing.stats.tid_map.remove(&TEST_TID);

            assert_eq!(domain_id, expected_domain_id);
        }
    }

    #[iluvatar_library::sim_test]
    async fn pick_from_system_domains_test() {
        let lbpolicy = &"guardrails".to_string();
        let durs = vec![0.001, 0.001, 10.0, 0.001];
        let expected_domains = vec![0, 0, 0, 1];

        test_domains(lbpolicy, durs, expected_domains).await;
    }

    #[iluvatar_library::sim_test]
    async fn pick_from_system_domains_nocpubound_test() {
        let lbpolicy = &"guardrailsnocpubound".to_string();
        let durs = vec![0.001, 0.001, 10.0, 0.001];
        let expected_domains = vec![0, 0, 0, 1];

        test_domains(lbpolicy, durs, expected_domains).await;
    }

    #[iluvatar_library::sim_test]
    async fn acquire_cores_with_preferred_honors_some_domain_over_po2() {
        use iluvatar_worker_library::services::resources::cpu::{build_load_avg_signal, CpuResourceTracker};
        use iluvatar_worker_library::worker_api::worker_config::CPUResourceConfig;

        let (_log, _cfg, _cm, _invoker, reg, cmap, _gpu) = build_test_services(None, None, None).await;
        let mut flb_config = FineLoadBalancingConfig::default();
        flb_config.dispatchpolicy = "power_of_two".to_string();
        flb_config.testing = 1;
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
            cmap,
            Some(Arc::new(flb_config)),
        )
        .unwrap();
        let func_reg = reg.register(cpu_reg(), &TEST_TID).await.unwrap();
        let flb = tracker.fineloadbalancing().unwrap();

        // Repeatedly pass preferred_domain = Some(3): must always land on domain 3, never Po2 random.
        for idx in 0..8 {
            let tid = format!("pref-tid-{idx}");
            let permit = tracker
                .acquire_cores_with_preferred(&func_reg, &tid, Some(3))
                .await
                .unwrap();
            assert!(permit.is_some());
            assert_eq!(flb.stats.tid_map.get(&tid).map(|v| *v), Some(3));
            drop(permit);
        }

        // When preferred_domain is None, falls back to power_of_two and records a valid domain.
        let permit = tracker
            .acquire_cores_with_preferred(&func_reg, &"fallback-tid".to_string(), None)
            .await
            .unwrap();
        assert!(permit.is_some());
        let assigned = flb.stats.tid_map.get(&"fallback-tid".to_string()).map(|v| *v);
        assert!(matches!(assigned, Some(0..=3)));
    }
}
