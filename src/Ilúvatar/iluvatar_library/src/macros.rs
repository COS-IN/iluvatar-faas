#[macro_export]
/// A helper macro to get the last item in the [crate::bail_error] macro, which will be the error message.
macro_rules! last {
  ([$single:tt] $($rest:tt)*) => {
    $single // base case
  };
  ([$first:tt $($rest:tt)*] $($reversed:tt)*) => {
    $crate::last!([$($rest)*] $first $($reversed)*)  // recursion
  };
}

#[macro_export]
/// A helper macro to log an error with deails, then raise the message as an error
///
/// # Example
/// ```
/// use iluvatar_library::bail_error;
///
/// fn fails() -> anyhow::Result<()> {
///   let tid = "test".to_string();
///   bail_error!(tid=%tid, "An unfixable error occured");
/// }
/// assert_eq!(fails().err().unwrap().to_string(), "An unfixable error occured");
/// ```
macro_rules! bail_error {
  ($($arg:tt)+) => {
    {
      tracing::error!($($arg)+);
      anyhow::bail!($crate::last!([$($arg)+]))
    }
  };
}
