//! A key-value storage engine.
//!
//! Features:
//! - Disk-based heap file storage with an in-memory page buffer pool.
//! - B+ tree indexes for faster range query and key lookups.
//! - 2PL and/or serial transactional concurrency control.
//! - Lock manager with table and row-level locks for decreased contention and
//!   optimized multi-agent performance.

mod disk;
mod lock;

const PAGE_SIZE_BYTES: usize = 4096;
