#[macro_export]
macro_rules! last {
  ([$single:tt] $($rest:tt)*) => { 
    $single // base case
  };
  ([$first:tt $($rest:tt)*] $($reversed:tt)*) => { 
    $crate::last!([$($rest)*] $first $($reversed)*)  // recursion
  };
}

#[macro_export]
/// test
macro_rules! bail_error {
  ($($arg:tt)+) => {
    {
      tracing::error!($($arg)+);
      anyhow::bail!($crate::last!([$($arg)+]))
    }
  };
}
