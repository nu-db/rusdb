//! The disk manager for the storage engine. Responsible for reading and writing to
//! database pages on disk.
mod disk_manager;

pub(crate) const DATA_DIR: &str = "src/disk/data/";
