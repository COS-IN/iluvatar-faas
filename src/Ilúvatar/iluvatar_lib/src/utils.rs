pub fn calculate_fqdn(function_name: &String, function_version: &String) -> String {
  format!("{}/{}", function_name, function_version)
}