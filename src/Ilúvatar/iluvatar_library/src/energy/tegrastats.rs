use crate::clock::now;
use crate::{bail_error, transaction::TransactionId, utils::execute_cmd_nonblocking};
use anyhow::Result;
use std::{process::Child, time::Duration};
use tracing::{info, warn};

/// Start tegrastat tracking of several metrics:
/// tegrastats --interval 1000 --logfile temp.log
/*
   Usage: tegrastats [-option]
   Options:
       --help                  : print this help screen
       --interval <millisec>   : sample the information in <milliseconds>
       --logfile  <filename>   : dump the output of tegrastats to <filename>
       --load_cfg <filename>   : load the information from <filename>
       --readall               : collect all stats including performance intensive stats
       --save_cfg <filename>   : save the information to <filename>
       --start                 : run tegrastats as a daemon process in the background
       --stop                  : stop any running instances of tegrastats
       --verbose               : print verbose message

*/
pub async fn start_tegrastats<S>(outfile: &S, tid: &TransactionId, stat_duration_ms: u64) -> Result<Child>
where
    S: AsRef<str> + ?Sized + std::fmt::Display,
{
    let st = stat_duration_ms.to_string();
    let args = vec!["--interval", &st.as_str(), "--logfile", outfile.as_ref()];
    test_args(tid, &args).await?;
    info!(tid=tid, tegrastats=?args, "tegrastat arguments");
    execute_cmd_nonblocking("/usr/bin/tegrastats", &args, None, tid)
}

async fn test_args(tid: &TransactionId, args: &Vec<&str>) -> Result<bool> {
    let mut child = execute_cmd_nonblocking("/usr/bin/tegrastats", args, None, tid)?;
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
