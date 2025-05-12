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

fn copy_folder(folder: &Path) {
    let output_path = get_output_path().join(folder);
    std::fs::create_dir_all(&output_path).unwrap();

    let pth = Path::new("src").join(folder);
    println!("cargo:rerun-if-changed={}", pth.to_string_lossy());
    for path in std::fs::read_dir(&pth).unwrap() {
        let path = path.unwrap();
        let pth_type = path.file_type().unwrap();
        if pth_type.is_dir() {
            copy_folder(Path::new(&folder).join(path.file_name()).as_path());
        } else if pth_type.is_file() {
            let input_path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
                .join("src")
                .join(folder)
                .join(path.file_name());
            let output_path = Path::new(&output_path).join(path.file_name());
            std::fs::copy(input_path, output_path).unwrap();
        }
    }
}

fn main() {
    copy_folder(Path::new("resources"));
}
