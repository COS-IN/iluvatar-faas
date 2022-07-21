#[macro_export]
macro_rules! bail_error {
  ($msg:expr, $($arg:tt)* ) => {
    {
      tracing::error!($msg, $($arg)*);
      anyhow::bail!($msg, $($arg)*)
    }
  };
}