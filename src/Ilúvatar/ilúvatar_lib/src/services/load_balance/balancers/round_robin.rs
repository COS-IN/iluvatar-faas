use crate::{services::load_balance::LoadBalancerTrait, load_balancer_api::structs::RegisterWorker};

pub struct RoundRobinLoadBalancer {

}

impl LoadBalancerTrait for RoundRobinLoadBalancer {
    fn add_worker(&self, _worker: &RegisterWorker) {
        todo!()
    }

    fn send_invocation(&self) {
        todo!()
    }

    fn update_worker_status(&self) {
        todo!()
    }
}