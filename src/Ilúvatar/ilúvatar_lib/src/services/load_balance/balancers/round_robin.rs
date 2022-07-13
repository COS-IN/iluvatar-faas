use crate::services::load_balance::LoadBalancer;

pub struct RoundRobinLoadBalancer {

}

impl LoadBalancer for RoundRobinLoadBalancer {
    fn register_worker(&self) {
        todo!()
    }

    fn send_invocation(&self) {
        todo!()
    }

    fn update_worker_status(&self) {
        todo!()
    }
}