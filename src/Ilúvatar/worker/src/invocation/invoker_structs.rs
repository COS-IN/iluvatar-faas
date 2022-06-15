#[derive(Debug)]
#[allow(unused)]
pub struct AsyncInvoke {
  pub completed: bool,
  pub result: Option<String>,
  pub duration: u64,
}
