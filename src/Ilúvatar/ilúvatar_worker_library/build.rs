use std::env;
use std::error::Error;
use std::path::Path;
use std::path::PathBuf;

fn get_output_path() -> PathBuf {
    //<root or manifest path>/target/<profile>/
    let manifest_dir_string = env::var("CARGO_MANIFEST_DIR").unwrap();
    let build_type = env::var("PROFILE").unwrap();
    let path = Path::new(&manifest_dir_string)
        .join("..")
        .join("target")
        .join(build_type);
    return PathBuf::from(path);
}

fn copy_folder(folder: &Path) -> Result<(), Box<dyn Error>> {
    let output_path = get_output_path().join(folder);
    std::fs::create_dir_all(&output_path).unwrap();

    for path in std::fs::read_dir(Path::new("src").join(folder)).unwrap() {
        let path = path.unwrap();
        let pth_type = path.file_type().unwrap();
        if pth_type.is_dir() {
            copy_folder(Path::new(&folder).join(path.file_name()).as_path()).unwrap();
        } else if pth_type.is_file() {
            let input_path = Path::new(&env::var("CARGO_MANIFEST_DIR").unwrap())
                .join("src")
                .join(folder)
                .join(path.file_name());
            let output_path = Path::new(&output_path).join(path.file_name());
            std::fs::copy(input_path, output_path).unwrap();
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        .type_attribute(
            "iluvatar_worker.LanguageRuntime",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_worker.SupportedIsolation",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_worker.SupportedCompute",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_worker.InvokeResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_worker.InvokeAsyncResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_worker.PrewarmResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_worker.RegisterResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_worker.StatusResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_worker.ContainerState",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .compile(&["src/rpc/iluvatar_worker.proto"], &["src"])?;
    copy_folder(Path::new("resources")).unwrap();
    Ok(())
}
