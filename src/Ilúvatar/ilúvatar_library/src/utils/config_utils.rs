pub fn args_to_json(args: &Vec<String>) -> String {
  let mut ret = String::from("{");
  for arg in args {
    if ! arg.contains("=") {
      panic!("Function argument '{}' does not contain an =", arg);
    }
    let split: Vec<&str> = arg.split("=").collect();
    if split.len() != 2 {
      panic!("Got unexpected number of items ({}) in argument '{}'; Should only have 2", split.len(), arg);
    }
    let fmt = format!("\"{}\":\"{}\"", split[0].to_string(), split[1].to_string());
    if ret.len() > 1 {
      ret.push(',');
    }
    ret.push_str(fmt.as_str());
  }
  ret.push('}');
  ret
}