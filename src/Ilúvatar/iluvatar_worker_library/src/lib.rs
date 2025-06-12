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

pub fn tar_folder<P: AsRef<std::path::Path>>(folder: P, tid: &TransactionId) -> anyhow::Result<Vec<u8>> {
    let file = std::io::Cursor::new(Vec::new());
    let mut a = tar::Builder::new(file);
    if let Err(e) = a.append_dir_all(".", folder) {
        bail_error!(tid=tid, error=%e, "failed to load dir");
    }
    match a.into_inner() {
        Err(e) => bail_error!(tid=tid, error=%e, "failed to finish building tar"),
        Ok(t) => Ok(t.into_inner()),
    }
}
pub fn unpack_tar<P: AsRef<std::path::Path>>(folder: P, data: &[u8], tid: &TransactionId) -> anyhow::Result<()> {
    match tar::Archive::new(data).unpack(&folder) {
        Err(e) => bail_error!(tid=tid, error=%e, "tar unpack failed"),
        Ok(_) => Ok(()),
    }
}

use iluvatar_library::bail_error;
use iluvatar_library::transaction::TransactionId;
pub use iluvatar_rpc::rpc::Runtime;

pub mod http;
pub mod services;
pub mod worker_api;
