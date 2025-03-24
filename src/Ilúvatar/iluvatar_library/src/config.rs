#[macro_export]
/// A macro to enable injecting compile-time configuration that will be used at load time.
/// Will still load from file and environment that override the defaults.
/// Should only be used for tests loading test-specific config where files might be moved and hard to find.
///
/// # Example
/// ```
/// #[derive(serde::Deserialize)]
/// struct CustomConfig {
///
/// }
/// pub fn new(
///     config_fpath: Option<&str>,
///     overrides: Option<Vec<(String, String)>>,
/// ) -> anyhow::Result<CustomConfig> {
///     iluvatar_library::load_config_default!(
///             "iluvatar_worker/src/worker.json",
///             config_fpath,
///             overrides,
///             "ILUVATAR_WORKER"
///         )
/// }
/// ```
macro_rules! load_config_default {
    ($defaults_json_file:literal, $overrides_config_fpath:ident, $overrides:ident, $env_prefix:expr) => {{
        let defaults = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../", $defaults_json_file));
        $crate::config::load_config(Some(defaults), $overrides_config_fpath, $overrides, $env_prefix)
    }};
}

pub fn load_config<T>(
    default_json: Option<&str>,
    overrides_config_fpath: Option<&str>,
    overrides: Option<Vec<(String, String)>>,
    env_prefix: &str,
) -> anyhow::Result<T>
where
    T: for<'a> serde::Deserialize<'a>,
{
    let mut builder = config::Config::builder();
    if let Some(default_json) = default_json {
        builder = builder.add_source(config::File::from_str(default_json, config::FileFormat::Json));
    }
    if let Some(config_fpath) = overrides_config_fpath {
        if std::path::Path::new(&config_fpath).exists() {
            builder = builder.add_source(config::File::with_name(config_fpath));
        }
    }
    builder = builder.add_source(
        config::Environment::with_prefix(env_prefix)
            .try_parsing(true)
            .separator("__")
            .prefix_separator("__"),
    );
    if let Some(overrides) = overrides {
        for (k, v) in overrides {
            builder = match builder.set_override(&k, v.clone()) {
                Ok(s) => s,
                Err(e) => {
                    anyhow::bail!("Failed to set override '{}' to '{}' because {}", k, v, e)
                },
            };
        }
    }
    match builder.build() {
        Ok(s) => match s.try_deserialize() {
            Ok(cfg) => Ok(cfg),
            Err(e) => anyhow::bail!("Failed to deserialize configuration because '{}'", e),
        },
        Err(e) => anyhow::bail!("Failed to build configuration because '{}'", e),
    }
}
