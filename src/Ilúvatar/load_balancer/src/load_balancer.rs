
pub struct LoadBalancer {
  
}

impl LoadBalancer {
  pub fn index(&self) {
    println!("INDEX");
  }
  pub fn name(&self, name_str: &String) {
    println!("server: {}", name_str);
  }
}