use crate::disk::DATA_DIR;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// Handles read and write accesses to pages stored on disk. File I/O operations are synchronous.
/// Asynchronous row operations, on the other hand, should occur on the pages buffered in memory,
/// with the disk manager being protected behind a [tokio::sync::RwLock] synchronization primitive.
pub struct DiskManager {
    current_page: std::sync::atomic::AtomicU32,
    reader: BufReader<std::fs::File>,
    writer: BufWriter<std::fs::File>,
}

impl DiskManager {
    /// Creates a new disk manager for the given database file `filename`, e.g. `example.db`.
    pub(crate) fn new(filename: &str) -> Self {
        let path = Path::new(DATA_DIR).join(filename);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&path)
            .expect(format!("Unable to create or open file {}.", path.display()).as_str());

        let reader = file;
        let writer = reader
            .try_clone()
            .expect(format!("Unable to clone reader for file {}.", path.display()).as_str());

        Self {
            current_page: std::sync::atomic::AtomicU32::new(0),
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::disk_manager::DiskManager;

    // Makes sure that we're able to open/create a file within the DATA_DIR directory.
    #[test]
    fn test_new() {
        let _ = DiskManager::new("test.db");
    }
}
