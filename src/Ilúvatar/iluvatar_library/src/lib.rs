//! Ilúvatar Library
//!
//! This crate is for shared code and utilities that are not specific to any executable in the Ilúvatar stack.
extern crate iluvatar_derive;
pub use iluvatar_derive::ToAny;
extern crate self as iluvatar_library;

pub mod transaction;
pub mod utils;
#[macro_use]
pub mod macros;
pub mod char_map;
pub mod clock;
pub mod config;
pub mod continuation;
pub mod energy;
pub mod influx;
mod linear_reg;
pub mod logging;
pub mod mindicator;
pub mod ring_buff;
pub mod threading;
pub mod tokio_utils;
pub mod tput_calc;
pub mod types;
