use crate::services::containers::clients::ContainerClient;
use crate::services::containers::structs::ParsedResult;
use anyhow::Result;
use iluvatar_library::clock::now;
use iluvatar_library::utils::file::container_path;
use iluvatar_library::{bail_error, transaction::TransactionId};
use std::path::PathBuf;
use std::{collections::HashMap, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, MutexGuard};

#[repr(u64)]
#[allow(unused)]
enum Command {
    Check = 0,
    Invoke = 1,
    ToDevice = 2,
    FromDevice = 3,
}
#[repr(C)]
struct SendMessage {
    command: Command,
    args_len_bytes: u64,
}
unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    core::slice::from_raw_parts((p as *const T) as *const u8, size_of::<T>())
}

type HeldSockLock<'a> = MutexGuard<'a, Option<UnixStream>>;
#[derive(Debug)]
pub struct SocketContainerClient {
    /// Behind a mutex to prevent concurrent messages/commands being sent, which the server cannot handle.
    socket: Mutex<Option<UnixStream>>,
    sock_pth: PathBuf,
    invoke_timeout: u64,
}
impl SocketContainerClient {
    pub async fn new(container_id: &str, invoke_timeout: u64, _tid: &TransactionId) -> Result<Self> {
        Ok(Self {
            socket: Mutex::new(None),
            sock_pth: container_path(container_id).join("sock"),
            invoke_timeout,
        })
    }

    /// Returns connected and held sock, or an error.
    /// Safe to unwrap option
    async fn get_socket(&self, tid: &TransactionId) -> Result<HeldSockLock<'_>> {
        let mut lck = self.socket.lock().await;
        if lck.is_none() {
            let start = now();
            tracing::info!(tid=tid, socket=?self.sock_pth, "waiting to open socket");
            loop {
                if let Ok(c) = tokio::fs::try_exists(&self.sock_pth).await {
                    if !c {
                        if start.elapsed() >= Duration::from_secs(self.invoke_timeout) {
                            bail_error!(tid = tid, "Failed to open Unix socket after timeout");
                        }
                        tokio::time::sleep(Duration::from_micros(1)).await;
                    } else {
                        break;
                    }
                }
            }
            let stream = match UnixStream::connect(&self.sock_pth).await {
                Ok(s) => s,
                Err(e) => bail_error!(tid=tid, error=%e, "failed to open Unix socket"),
            };
            *lck = Some(stream);
        }
        Ok(lck)
    }

    async fn send_command(
        &self,
        sock: &mut UnixStream,
        cmd: SendMessage,
        tid: &TransactionId,
        container_id: &str,
        bytes: Option<&[u8]>,
    ) -> Result<()> {
        let init_bytes = unsafe { any_as_u8_slice(&cmd) };
        match sock.write(init_bytes).await {
            Ok(_) => (),
            Err(e) => bail_error!(tid=tid, container_id=container_id, error=%e, "failed sending command metadata"),
        };
        if let Some(bytes) = bytes {
            match sock.write(bytes).await {
                Ok(_) => (),
                Err(e) => bail_error!(tid=tid, container_id=container_id, error=%e, "failed sending command arguments"),
            };
        }
        Ok(())
    }

    async fn recv_result(&self, sock: &mut UnixStream, tid: &TransactionId, container_id: &str) -> Result<Box<[u8]>> {
        #[cfg(target_endian = "big")]
        let size = match sock.read_u64().await {
            Ok(size) => size as usize,
            Err(e) => bail_error!(tid=tid, error=%e, "failed getting return size"),
        };
        #[cfg(target_endian = "little")]
        let size = match sock.read_u64_le().await {
            Ok(size) => size as usize,
            Err(e) => bail_error!(tid=tid, error=%e, "failed getting return size"),
        };
        let mut buff: Box<[u8]> = vec![0; size].into_boxed_slice();
        match sock.read_exact(buff.as_mut()).await {
            Ok(_) => (),
            Err(e) => bail_error!(tid=tid, container_id=container_id, error=%e, "Failed reading return results"),
        };
        Ok(buff)
    }

    async fn internal_invoke(
        &self,
        json_args: &str,
        tid: &TransactionId,
        container_id: &str,
    ) -> Result<(ParsedResult, Duration)> {
        let start = now();

        let bytes = json_args.as_bytes();
        let mut sock = self.get_socket(tid).await?;
        let invoke_cmd = SendMessage {
            command: Command::Invoke,
            args_len_bytes: bytes.len() as u64,
        };
        self.send_command(sock.as_mut().unwrap(), invoke_cmd, tid, container_id, Some(bytes))
            .await?;

        let buff = self.recv_result(sock.as_mut().unwrap(), tid, container_id).await?;
        drop(sock);
        let result = ParsedResult::parse_slice(&buff, tid)?;

        Ok((result, start.elapsed()))
    }

    fn check_driver_status(&self, tid: &TransactionId, slice: &[u8]) -> Result<()> {
        match serde_json::from_slice::<HashMap<String, i32>>(slice) {
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
                None => {
                    bail_error!(tid=tid, result=?std::str::from_utf8(slice), "Return didn't have driver status result")
                },
            },
            Err(e) => {
                bail_error!(error=%e, tid=tid, result=?std::str::from_utf8(slice), "Failed to parse json from HTTP return")
            },
        }
    }
}

#[tonic::async_trait]
impl ContainerClient for SocketContainerClient {
    #[tracing::instrument(skip(self, json_args), fields(tid=tid), name="SocketContainerClient::invoke")]
    async fn invoke(
        &self,
        json_args: &str,
        tid: &TransactionId,
        container_id: &str,
    ) -> Result<(ParsedResult, Duration)> {
        // Some of the socket functions aren't 'cancellation-safe' (i.e. the writes can be mid-action),
        // but if the timeout is reached the container is going to be marked for removal anyway.
        match tokio::time::timeout(
            Duration::from_secs(self.invoke_timeout),
            self.internal_invoke(json_args, tid, container_id),
        )
        .await
        {
            Ok(res) => res,
            Err(elapsed) => {
                bail_error!(tid=tid, container_id=container_id, elapsed=%elapsed, "Timeout waiting for socket read/write")
            },
        }
    }

    async fn move_to_device(&self, tid: &TransactionId, container_id: &str) -> Result<()> {
        let mut sock = self.get_socket(tid).await?;
        let invoke_cmd = SendMessage {
            command: Command::ToDevice,
            args_len_bytes: 0,
        };
        self.send_command(sock.as_mut().unwrap(), invoke_cmd, tid, container_id, None)
            .await?;
        let buff = self.recv_result(sock.as_mut().unwrap(), tid, container_id).await?;
        drop(sock);
        self.check_driver_status(tid, &buff)
    }
    async fn move_from_device(&self, tid: &TransactionId, container_id: &str) -> Result<()> {
        tracing::info!(tid = tid, "moving container data off device");
        let mut sock = self.get_socket(tid).await?;
        let invoke_cmd = SendMessage {
            command: Command::FromDevice,
            args_len_bytes: 0,
        };
        self.send_command(sock.as_mut().unwrap(), invoke_cmd, tid, container_id, None)
            .await?;
        let buff = self.recv_result(sock.as_mut().unwrap(), tid, container_id).await?;
        drop(sock);
        self.check_driver_status(tid, &buff)
    }
}
