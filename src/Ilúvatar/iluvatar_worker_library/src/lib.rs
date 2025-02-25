#[macro_export]
/// A helper macro to log an error with details, then raise the message as an error
///
/// # Example
/// ```
///    let inv = worker.invoke(request).instrument(tracing::error_span!("enter_worker", worker=self.worker.config.name));
/// ```
macro_rules! name_span {
    ($name:expr) => {
        tracing::error_span!("enter_worker", worker = $name)
    };
}

pub mod services;
pub mod worker_api;
