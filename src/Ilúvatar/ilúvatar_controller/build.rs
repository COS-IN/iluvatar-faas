use std::env;
use std::path::Path;
use std::path::PathBuf;
use std::error::Error;

fn get_output_path() -> PathBuf {
  //<root or manifest path>/target/<profile>/
  let manifest_dir_string = env::var("CARGO_MANIFEST_DIR").unwrap();
  let build_type = env::var("PROFILE").unwrap();
  let path = Path::new(&manifest_dir_string).join("..").join("target").join(build_type);
  return PathBuf::from(path);
}

fn copy_file(infile: &Path) -> Result<(), Box<dyn Error>> {
  let output_path = get_output_path().join(infile.file_name().unwrap());
  let infile = Path::new("src").join(infile);
  std::fs::copy(infile, output_path).unwrap();
  Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
  copy_file(Path::new("controller.json")).unwrap();
  Ok(())
}
