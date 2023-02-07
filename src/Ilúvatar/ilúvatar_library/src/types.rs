use serde::{Serialize, Deserialize};

pub type MemSizeMb = i64;

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CommunicationMethod {
  RPC,
  SIMULATION
}