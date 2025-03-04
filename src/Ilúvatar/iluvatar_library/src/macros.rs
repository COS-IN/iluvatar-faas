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
/// A helper macro to log an error with details, then raise the message as an error
///
/// # Example
/// ```
/// use iluvatar_library::bail_error;
///
/// fn fails() -> anyhow::Result<()> {
///   let tid = "test".to_string();
///   bail_error!(tid=tid, "An unfixable error occured");
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

#[macro_export]
/// Helper for [error_value]
macro_rules! error_value_recurr {
  ([$single:tt] $($rest:tt)*) => {
    return $crate::types::err_val(anyhow::format_err!($($rest)*), $single);
  };
  ([$first:tt $($rest:tt)*] $($items:tt)*) => {
    $crate::error_value_recurr!([$($rest)*] $($items)* $first)  // recursion
  };
}
#[macro_export]
/// Format an error and return the [ResultErrorVal] result
///
/// # Example
/// ```
/// use iluvatar_library::error_value;
/// use iluvatar_library::types::ResultErrorVal;
/// let owned_input = 32;
///
/// fn fail(owned_input: i32) -> ResultErrorVal<(), i32> {
///     error_value!("Input was {:?}", owned_input, owned_input);
/// }
/// match fail(owned_input) {
///   Ok(_) => assert!(false, "Function should have returned an error"),
///   Err((e, returned_input)) => {
///     assert_eq!(owned_input, returned_input);
///     assert_eq!(e.to_string(), "Input was 32");
///   },
/// }
/// ```
macro_rules! error_value {
  ($($arg:tt)+) => {
    $crate::error_value_recurr!([$($arg)+])
  };
}

#[macro_export]
/// Helper for [bail_error_value]
macro_rules! bail_error_value_recurr {
  ([$msg:literal $comma:tt $value:ident] $($rest:tt)*) => {
    tracing::error!($($rest)* $msg);
    return $crate::types::err_val(anyhow::format_err!($msg), $value)
  };
  ([$msg:literal $comma:tt $value:expr] $($rest:tt)*) => {
    tracing::error!($($rest)* $msg);
    return $crate::types::err_val(anyhow::format_err!($msg), $value)
  };
  ([$first:tt $penultimate:tt $($rest:tt)+] $($items:tt)*) => {
    $crate::bail_error_value_recurr!([$penultimate $($rest)+] $($items)* $first)  // recursion
  };
}

#[macro_export]
/// A helper macro to log an error with details, then raise the message as an error.
/// Returns a [ResultErrorVal] device_resource
///
/// # Example
/// ```
/// use iluvatar_library::bail_error_value;
/// use iluvatar_library::types::ResultErrorVal;
///
/// fn fails(owned_input: i32) -> ResultErrorVal<(),i32> {
///   let tid = "test".to_string();
///   bail_error_value!(tid=tid, "An unfixable error occurred", owned_input);
/// }
/// let input = 60;
/// match fails(60) {
///   Ok(_) => assert!(false, "Function should have returned an error"),
///   Err((e, returned_input)) => {
///     assert_eq!(input, returned_input, "Owned input was not returned correctly");
///     assert_eq!(e.to_string(), "An unfixable error occurred");
///   },
/// }
///
/// ```
macro_rules! bail_error_value {
  ($($arg:tt)+) => {
    $crate::bail_error_value_recurr!([$($arg)+])
  };
}
