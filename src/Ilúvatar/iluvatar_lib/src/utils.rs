/// get the fully qualified domain name for a function from its name and version
pub fn calculate_fqdn(function_name: &String, function_version: &String) -> String {
  format!("{}/{}", function_name, function_version)
}