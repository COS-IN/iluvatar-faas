use crate::services::load_balance::LoadBalancer;

pub struct LeastLoadedBalancer {

}

impl LoadBalancer for LeastLoadedBalancer {
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