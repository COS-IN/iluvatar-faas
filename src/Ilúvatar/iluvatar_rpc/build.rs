use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        .type_attribute(
            "iluvatar_rpc.LanguageRuntime",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_rpc.SupportedIsolation",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_rpc.SupportedCompute",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_rpc.InvokeResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_rpc.InvokeAsyncResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_rpc.PrewarmResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_rpc.RegisterResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_rpc.StatusResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_rpc.ContainerState",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .compile_protos(&["src/rpc/iluvatar_rpc.proto"], &["src"])?;
    Ok(())
}
