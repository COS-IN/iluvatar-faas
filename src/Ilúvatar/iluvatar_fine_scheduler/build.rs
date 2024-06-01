// This software may be used and distributed according to the terms of the
// GNU General Public License version 2.
//
fn get_output_path() -> PathBuf {
    //<root or manifest path>/target/<profile>/
    let manifest_dir_string = env::var("CARGO_MANIFEST_DIR").unwrap();
    let build_type = env::var("PROFILE").unwrap();
    let path = Path::new(&manifest_dir_string)
        .join("..")
        .join("target")
        .join(build_type);
    path
}

fn copy_file(infile: &Path) -> Result<(), Box<dyn Error>> {
    let output_path = get_output_path().join(infile.file_name().unwrap());
    let infile = Path::new("examples").join(infile);
    std::fs::copy(infile, output_path).unwrap();
    Ok(())
}

fn main() {
    //scx_rustland_core::RustLandBuilder::new()
    //    .unwrap()
    //    .build()
    //    .unwrap();
    scx_utils::BpfBuilder::new()
        .unwrap()
        .enable_intf("src/bpf/intf.h", "bpf_intf.rs")
        .enable_skel("src/bpf/main.bpf.c", "bpf")
        .build()
        .unwrap();

    copy_file(Path::new("characteristics.csv")).unwrap();
    copy_file(Path::new("pids.csv")).unwrap();
}

