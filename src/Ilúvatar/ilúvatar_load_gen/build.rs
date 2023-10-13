use std::env;
use std::error::Error;
use std::fs::create_dir_all;
use std::path::Path;
use std::path::PathBuf;

fn get_output_path() -> PathBuf {
    //<root or manifest path>/target/<profile>/
    let manifest_dir_string = env::var("CARGO_MANIFEST_DIR").unwrap();
    let build_type = env::var("PROFILE").unwrap();
    let path = Path::new(&manifest_dir_string)
        .join("..")
        .join("target")
        .join(build_type)
        .join("resources");
    return PathBuf::from(path);
}

fn copy_file(infile: &Path) -> Result<(), Box<dyn Error>> {
    let output_path = get_output_path();
    let output_file = output_path.join(infile.file_name().unwrap());
    let infile = Path::new("src").join("resources").join(infile);
    create_dir_all(&output_path)?;
    std::fs::copy(infile, output_file).unwrap();
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    copy_file(Path::new("packaged-benchmark.csv")).unwrap();
    copy_file(Path::new("gpu-benchmark.csv")).unwrap();
    copy_file(Path::new("cpu-benchmark.csv")).unwrap();
    Ok(())
}
