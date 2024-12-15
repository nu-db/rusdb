//! Module for custom error-handling of recoverable errors in Rustdb crates.crates.
mod error;
mod macros;

pub use error::{Error, Result};
pub use macros::*;
