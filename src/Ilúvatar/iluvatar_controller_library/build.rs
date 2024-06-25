use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        .type_attribute(
            "iluvatar_controller.LanguageRuntime",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_controller.SupportedIsolation",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_controller.SupportedCompute",
            "#[derive(serde::Serialize,serde::Deserialize,clap::ValueEnum)]",
        )
        .type_attribute(
            "iluvatar_controller.InvokeResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_controller.InvokeAsyncResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_controller.PrewarmResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_controller.RegisterResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_controller.StatusResponse",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .type_attribute(
            "iluvatar_controller.ContainerState",
            "#[derive(serde::Serialize,serde::Deserialize)]",
        )
        .compile(&["src/rpc/iluvatar_controller.proto"], &["src"])?;
    Ok(())
}
