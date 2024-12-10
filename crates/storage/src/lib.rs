//! A key-value storage engine.
//!
//! Features:
//! - Disk-based heap file storage with an in-memory page buffer pool.
//! - B+ tree indexes for faster range query and key lookups.
//! - 2PL and/or serial transactional concurrency control.
