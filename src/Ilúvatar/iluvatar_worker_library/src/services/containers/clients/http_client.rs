use crate::services::containers::clients::ContainerClient;
use crate::services::containers::structs::ParsedResult;
use anyhow::Result;
use iluvatar_library::clock::now;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    utils::{calculate_base_uri, calculate_invoke_uri, format_uri, port::Port},
};
use reqwest::{Client, Response, StatusCode};
use std::{collections::HashMap, time::Duration};
use tracing::warn;

#[derive(Debug)]
pub struct HttpContainerClient {
    _port: Port,
    invoke_uri: String,
    _base_uri: String,
    move_to_dev: String,
    move_to_host: String,
    client: Client,
}

impl HttpContainerClient {
    pub fn new(
        container_id: &str,
        port: Port,
        address: &str,
        invoke_timeout: u64,
        tid: &TransactionId,
    ) -> Result<Self> {
        let client = match reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .pool_idle_timeout(None)
            // tiny buffer to allow for network delay from possibly full system
            .connect_timeout(Duration::from_secs(invoke_timeout + 2))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                bail_error!(tid=tid, error=%e, container_id=%container_id, "Unable to build reqwest HTTP client")
            },
        };
        Ok(Self {
            _port: port,
            client,
            invoke_uri: calculate_invoke_uri(address, port),
            _base_uri: calculate_base_uri(address, port),
            move_to_dev: format_uri(address, port, "prefetch_stream_dev"),
            move_to_host: format_uri(address, port, "prefetch_stream_host"),
        })
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, json_args, container_id), fields(tid=tid)))]
    async fn call_container(
        &self,
        json_args: &str,
        tid: &TransactionId,
        container_id: &str,
    ) -> Result<(Response, Duration)> {
        let builder = self
            .client
            .post(&self.invoke_uri)
            .body(json_args.to_owned())
            .header("Content-Type", "application/json");
        let start = now();
        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid=tid, inner=std::error::Error::source(&e),
                    status=?e.status(), error=%e, container_id=%container_id,
                    "HTTP error when trying to connect to container");
            },
        };
        Ok((response, start.elapsed()))
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, response, container_id), fields(tid=tid)))]
    async fn download_text(&self, response: Response, tid: &TransactionId, container_id: &str) -> Result<String> {
        match response.text().await {
            Ok(r) => Ok(r),
            Err(e) => {
                bail_error!(tid=tid, error=%e, container_id=%container_id, "Error reading text data from container")
            },
        }
    }

    fn check_http_status(&self, tid: &TransactionId, status: StatusCode, text: &str, container_id: &str) -> Result<()> {
        match status {
            StatusCode::OK => Ok(()),
            StatusCode::UNPROCESSABLE_ENTITY => {
                warn!(tid=tid, status=StatusCode::UNPROCESSABLE_ENTITY.as_u16(), result=%text, container_id=%container_id, "A user code error occured in the container");
                Ok(())
            },
            StatusCode::INTERNAL_SERVER_ERROR => {
                bail_error!(tid=tid, status=StatusCode::INTERNAL_SERVER_ERROR.as_u16(), result=%text, container_id=%container_id, "A platform error occured in the container");
            },
            other => {
                bail_error!(tid=tid, status=%other, result=%text, container_id=%container_id, "Unknown status code from container call");
            },
        }
    }

    fn check_driver_status(&self, tid: &TransactionId, text: &str) -> Result<()> {
        match serde_json::from_str::<HashMap<String, i32>>(text) {
            Ok(p) => match p.get("Status") {
                Some(code) => {
                    match code {
                        0 => Ok(()),
                        // these error codes are converted CUresult codes
                        // 3 == CUDA_ERROR_NOT_INITIALIZED, so container is probably just created and hasn't used driver yet
                        3 => Ok(()),
                        _ => bail_error!(tid = tid, code = code, "Return had non-zero status code"),
                    }
                },
                None => bail_error!(tid=tid, result=%text, "Return didn't have driver status result"),
            },
            Err(e) => bail_error!(error=%e, tid=tid, result=%text, "Failed to parse json from HTTP return"),
        }
    }
}

#[tonic::async_trait]
impl ContainerClient for HttpContainerClient {
    #[tracing::instrument(skip(self, json_args, container_id), fields(tid=tid), name="HttpContainerClient::invoke")]
    async fn invoke(
        &self,
        json_args: &str,
        tid: &TransactionId,
        container_id: &str,
    ) -> Result<(ParsedResult, Duration)> {
        let (response, duration) = self.call_container(json_args, tid, container_id).await?;
        let status = response.status();
        let text = self.download_text(response, tid, container_id).await?;
        let result = ParsedResult::parse(&text, tid)?;
        self.check_http_status(tid, status, &text, container_id)?;
        Ok((result, duration))
    }

    async fn move_to_device(&self, tid: &TransactionId, container_id: &str) -> Result<()> {
        let builder = self
            .client
            .put(&self.move_to_dev)
            .header("Content-Type", "application/json");
        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid=tid, inner=std::error::Error::source(&e),
                    status=?e.status(), error=%e, container_id=%container_id,
                    "HTTP error when trying to connect to container");
            },
        };
        let status = response.status();
        let text = self.download_text(response, tid, container_id).await?;
        self.check_http_status(tid, status, &text, container_id)?;
        self.check_driver_status(tid, &text)
    }

    async fn move_from_device(&self, tid: &TransactionId, container_id: &str) -> Result<()> {
        let builder = self
            .client
            .put(&self.move_to_host)
            .header("Content-Type", "application/json");
        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid=tid, inner=std::error::Error::source(&e),
                    status=?e.status(), error=%e, container_id=%container_id,
                    "HTTP error when trying to connect to container");
            },
        };
        let status = response.status();
        let text = self.download_text(response, tid, container_id).await?;
        self.check_http_status(tid, status, &text, container_id)?;
        self.check_driver_status(tid, &text)
    }
}
