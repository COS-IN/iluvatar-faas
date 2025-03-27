#[macro_export]
/// A helper macro to log an error with details, then raise the message as an error
///
/// # Example
/// ```rust
/// use std::time::Duration;
/// use tracing::Instrument;
/// let span = iluvatar_worker_library::name_span!("worker_1");
/// tokio_test::block_on(async move {
///     // span "enter_worker" will be active here
///     tokio::time::sleep(Duration::from_secs(1)).await
/// }.instrument(span));
///
/// ```
macro_rules! name_span {
    ($name:expr) => {
        tracing::info_span!("enter_worker", worker = $name)
    };
}

pub mod http;
pub mod services;
pub mod worker_api;
