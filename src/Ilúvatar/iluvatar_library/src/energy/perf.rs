use crate::clock::now;
use crate::{bail_error, transaction::TransactionId, utils::execute_cmd_nonblocking};
use anyhow::Result;
use std::{process::Child, time::Duration};
use tracing::{debug, info, warn};

/// Start perf stat tracking of several metrics:
///  [power/energy-pkg/](https://stackoverflow.com/questions/55956287/perf-power-consumption-measure-how-does-it-work)
///  [power/energy-ram]
///  Instructions
///  L3 cache hits and misses
///  all cache references and misses
/// The csv results will be put into [outfile](https://manpages.ubuntu.com/manpages/bionic/man1/perf-stat.1.html)
pub async fn start_perf_stat<S>(outfile: &S, tid: &TransactionId, stat_duration_ms: u64) -> Result<Child>
where
    S: AsRef<str> + ?Sized + std::fmt::Display,
{
    let st = stat_duration_ms.to_string();
    let mut args = vec!["stat", "-I", st.as_str(), "-x", ",", "--output", outfile.as_ref()];
    try_add_arg(tid, "Added Instructions", "-M", "Instructions", &mut args).await?;
    try_add_arg(tid, "Added power/energy-ram/", "-e", "power/energy-ram/", &mut args).await?;
    try_add_arg(tid, "Added power/energy-pkg/", "-e", "power/energy-pkg/", &mut args).await?;
    try_add_arg(
        tid,
        "Added mem_load_retired.l3_hit",
        "-e",
        "mem_load_retired.l3_hit",
        &mut args,
    )
    .await?;
    try_add_arg(
        tid,
        "Added mem_load_retired.l3_miss",
        "-e",
        "mem_load_retired.l3_miss",
        &mut args,
    )
    .await?;
    try_add_arg(
        tid,
        "Added cpu cache references",
        "-e",
        "cpu/cache-references/",
        &mut args,
    )
    .await?;
    try_add_arg(tid, "Added cpu cache misses", "-e", "cpu/cache-misses/", &mut args).await?;
    info!(tid=tid, perf=?args, "perf arguments");
    execute_cmd_nonblocking("/usr/bin/perf", &args, None, tid)
}

async fn try_add_arg<'a>(
    tid: &TransactionId,
    msg: &str,
    flag: &'a str,
    metric: &'a str,
    command: &mut Vec<&'a str>,
) -> Result<()> {
    let args = vec!["stat", flag, metric, "-I", "100"];
    if test_args(tid, &args).await? {
        debug!(tid = tid, msg);
        command.push(flag);
        command.push(metric);
    }
    Ok(())
}

async fn test_args(tid: &TransactionId, args: &Vec<&str>) -> Result<bool> {
    let mut child = execute_cmd_nonblocking("/usr/bin/perf", args, None, tid)?;
    let start = now();

    let timeout = Duration::from_secs(1);
    while start.elapsed() < timeout {
        match child.try_wait() {
            Ok(exit) => match exit {
                // an exit means the metric doesn't exist
                Some(_) => return Ok(false),
                None => {
                    // didn't exit yet
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                },
            },
            Err(e) => {
                warn!(tid=tid, error=%e, "Checking if `{:?}` args existed encountered an error", args);
                return Ok(false);
            },
        };
    }
    // probably would have errored out after a second
    // safe to assume metric exists
    match child.kill() {
        Ok(_) => (),
        Err(e) => bail_error!(tid=tid, error=%e, "Failed to kill perf child when testing args"),
    };
    Ok(true)
}
