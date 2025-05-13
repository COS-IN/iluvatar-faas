use std::env;
use std::path::Path;
use std::path::PathBuf;

fn get_output_path() -> PathBuf {
    //<root or manifest path>/target/<build_target>/<profile>/
    let manifest_dir_string = env::var("CARGO_MANIFEST_DIR").unwrap();
    let build_type = env::var("OUT_DIR")
        .unwrap()
        .split(std::path::MAIN_SEPARATOR)
        .nth_back(3)
        .unwrap()
        .to_string();
    let build_target = env::var("TARGET").unwrap();
    Path::new(&manifest_dir_string)
        .join("..")
        .join("target")
        .join(build_target)
        .join(build_type)
}

fn copy_file(infile: &Path) {
    let output_path = get_output_path().join(infile.file_name().unwrap());
    let infile = Path::new("src").join(infile);
    println!("cargo:rerun-if-changed={}", infile.to_string_lossy());
    std::fs::copy(infile, output_path).unwrap();
}

fn main() {
    copy_file(Path::new("controller.json"));
}
