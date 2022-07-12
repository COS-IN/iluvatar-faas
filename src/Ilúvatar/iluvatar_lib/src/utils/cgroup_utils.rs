use super::file::TEMP_DIR;

pub fn cgroup_namespace(name: &str) -> String {
  format!("{}/cgroups/{}", TEMP_DIR, name)
}
