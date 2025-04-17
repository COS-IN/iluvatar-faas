use super::{queueing::EnqueuedInvocation, InvocationResultPtr};
use anyhow::Result;
use dashmap::DashMap;
use guid_create::GUID;
use iluvatar_library::transaction::TransactionId;
use iluvatar_rpc::rpc::{ContainerState, InvokeResponse};
use std::sync::Arc;

pub struct AsyncHelper {
    pub async_functions: Arc<DashMap<String, InvocationResultPtr>>,
}
unsafe impl Send for AsyncHelper {}
impl Default for AsyncHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncHelper {
    pub fn new() -> Self {
        AsyncHelper {
            async_functions: Arc::new(DashMap::new()),
        }
    }

    pub fn insert_async_invoke(&self, invoke: Arc<EnqueuedInvocation>) -> Result<String> {
        let cookie = GUID::rand().to_string();
        self.async_functions.insert(cookie.clone(), invoke.result_ptr.clone());

        Ok(cookie)
    }

    /// returns the async invoke entry if it exists
    /// None otherwise
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, cookie), fields(tid=_tid)))]
    fn get_async_entry(&self, cookie: &str, _tid: &TransactionId) -> Option<InvocationResultPtr> {
        let i = self.async_functions.get(cookie);
        i.map(|i| i.clone())
    }

    /// removes the async invoke entry from the tracked invocations
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, cookie), fields(tid=_tid)))]
    fn remove_async_entry(&self, cookie: &str, _tid: &TransactionId) {
        self.async_functions.remove(cookie);
    }

    /// polls the invocation status
    /// Destructively returns results if they are found
    /// returns a JSON blob of "{ "Error": "Invocation not found" }" if the invocation is not found
    /// returns a JSON blob of "{ "Status": "Invocation not completed" }" if the invocation has not completed yet
    /// NOTE: If these keys for non-completion are changed, then the controller will need modification to match
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, cookie), fields(tid=tid)))]
    pub fn invoke_async_check(&self, cookie: &str, tid: &TransactionId) -> Result<InvokeResponse> {
        let entry = match self.get_async_entry(cookie, tid) {
            Some(entry) => entry,
            None => return Ok(InvokeResponse::error("Invocation not found")),
        };

        let entry = entry.lock();
        if entry.completed {
            self.remove_async_entry(cookie, tid);
            return Ok(InvokeResponse {
                json_result: entry.result_json.to_string(),
                success: true,
                duration_us: entry.duration.as_micros() as u64,
                compute: entry.compute.bits(),
                container_state: entry.container_state.into(),
            });
        }
        Ok(InvokeResponse {
            json_result: "{ \"Status\": \"Invocation not completed\" }".to_string(),
            success: false,
            duration_us: 0,
            compute: iluvatar_library::types::Compute::empty().bits(),
            container_state: ContainerState::Error.into(),
        })
    }
}
