use tracing::trace;

use crate::{utils::execute_cmd, transaction::TransactionId};

pub struct IPMI {
  ipmi_pass_file: String,
  ipmi_ip_addr: String
}

impl IPMI {
  pub fn new(ipmi_pass_file: String, ipmi_ip_addr: String, tid: &TransactionId) -> anyhow::Result<Self> {
    let i = IPMI {
      ipmi_pass_file,
      ipmi_ip_addr,
    };

    // test settings
    i.read(tid)?;
    Ok(i)
  }

  /// Get the instantaneous wattage usage of the system from ipmi
  pub fn read(&self, tid: &TransactionId) -> anyhow::Result<u128> {
    trace!(tid=%tid, "Reading from ipmi");
    let output = execute_cmd("/usr/bin/ipmitool", &vec!["-f", self.ipmi_pass_file.as_str(), "-I", "lanplus",
                                                       "-H", self.ipmi_ip_addr.as_str(), "-U", "ADMIN", "dcmi", "power", "reading"], None, tid)?;
    match output.status.code() {
      Some(0) => (),
      Some(code) => anyhow::bail!("Got an illegal exit code from command '{}', output: {:?}", code, output),
      None => anyhow::bail!("Got no exit code from command, output: {:?}", output),
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut split = stdout.split("\n");
    match split.nth(1) {
      Some(instant_line) => {
        let strs: Vec<&str> = instant_line.split(" ").filter(|str| str.len() > 0).collect();
        if strs.len() == 5 {
          let watts = strs[3];
          Ok(watts.parse::<u128>()?)
        } else {
          anyhow::bail!("Instantaneous wattage line was the incorrect size, was: '{:?}'", strs)
        }
      },
      None => {
        anyhow::bail!("Stdout was too short, got '{}'", stdout)
      },
    }
  }
}
