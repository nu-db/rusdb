use crate::disk::DATA_DIR;
use crate::PAGE_SIZE_BYTES;
use bytes::{Bytes, BytesMut};
use rustdb_error::{errdata, Error, Result};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) type PageId = u64;

const EMPTY_BUFFER: &'static [u8] = &[0; PAGE_SIZE_BYTES];

/// Handles read and write accesses to pages stored on disk. File I/O operations are synchronous.
/// Asynchronous row operations, on the other hand, should occur on the pages buffered in memory,
/// with the disk manager being protected behind a [tokio::sync::RwLock] synchronization primitive.
#[derive(Debug)]
pub struct DiskManager {
    last_allocated_pid: std::sync::atomic::AtomicU64,
    reader: BufReader<std::fs::File>,
    writer: BufWriter<std::fs::File>,
}

impl DiskManager {
    /// Creates a new disk manager for the given database file `filename`, e.g. `example.db`.
    pub(crate) fn new(filename: &str) -> Result<Self> {
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

        let mut disk_manager = Self {
            last_allocated_pid: std::sync::atomic::AtomicU64::new(0),
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        };

        // Initialize the first page, potentially clearing out any garbage data.
        disk_manager.write(&0, EMPTY_BUFFER)?;

        Ok(disk_manager)
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        // `fetch_add` increments the current value and returns the old value.
        let page_id = 1 + self
            .last_allocated_pid
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        self.write(&page_id, EMPTY_BUFFER)?;
        Ok(page_id)
    }

    pub(crate) fn read(&mut self, page_id: &PageId) -> Result<Bytes> {
        self.reader
            .seek(SeekFrom::Start(Self::calculate_offset(page_id)?))?;
        let mut bytes = BytesMut::zeroed(PAGE_SIZE_BYTES);
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes.freeze())
    }

    pub(crate) fn write(&mut self, page_id: &PageId, data: &[u8]) -> Result<()> {
        if data.len() > PAGE_SIZE_BYTES {
            return errdata!("Page data must fit in a page.");
        }

        let offset = Self::calculate_offset(page_id)?;
        self.writer.seek(SeekFrom::Start(offset))?;
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }

    fn calculate_offset(page_id: &PageId) -> Result<u64> {
        match (*page_id).checked_mul(PAGE_SIZE_BYTES as u64) {
            Some(value) => Ok(value),
            None => Err(Error::ArithmeticOverflow),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::disk_manager::{DiskManager, EMPTY_BUFFER};
    use crate::PAGE_SIZE_BYTES;
    use bytes::{Buf, BufMut};
    use std::sync::atomic::Ordering::SeqCst;

    #[test]
    fn test_new() {
        // We're able to open/create a file within the DATA_DIR directory.
        let mut disk_manager = DiskManager::new("test.db").unwrap();

        // The page of a newly initialized disk manager should be of size `PAGE_SIZE_BYTES` filled
        // with 0 bytes, and should have a PageId of 0.
        let page_id = disk_manager.last_allocated_pid.load(SeqCst);
        assert_eq!(page_id, 0);
        let page = disk_manager.read(&page_id).unwrap();
        assert_eq!(page.len(), PAGE_SIZE_BYTES);
        assert_eq!(page.as_ref(), EMPTY_BUFFER);
    }

    #[test]
    fn test_allocate_page() {
        let mut disk_manager = DiskManager::new("test.db").unwrap();

        // `allocate_page()` should increment the current PageId and return the new one.
        let page_id = disk_manager.allocate_page().unwrap();
        assert_eq!(page_id, 1);
        assert_eq!(disk_manager.last_allocated_pid.load(SeqCst), page_id);

        // The allocated page corresponding to the new PageId should be of size `PAGE_SIZE_BYTES`,
        // filled with 0 bytes.
        let page = disk_manager.read(&page_id).unwrap();
        assert_eq!(page.len(), PAGE_SIZE_BYTES);
        assert_eq!(page.as_ref(), EMPTY_BUFFER);
    }

    #[test]
    fn test_page_access() {
        let mut disk_manager = DiskManager::new("test.db").unwrap();
        let mut buffer = Vec::new();

        // We should be able to write floats to the first page and read them back.
        let float_vals: Vec<f64> = (0..100).map(|i| i as f64 * 1.1).collect();
        float_vals.iter().for_each(|f| buffer.put_f64(*f));
        disk_manager.write(&0, &buffer).unwrap();

        let mut first_page = disk_manager.read(&0).unwrap();
        float_vals
            .iter()
            .for_each(|f| assert_eq!(first_page.get_f64(), *f));
        buffer.clear();

        // Create a new page. Try writing integers this time.
        disk_manager.allocate_page().unwrap();
        let int_vals: Vec<i32> = (0..100).map(|i| i).collect();
        int_vals.iter().for_each(|i| buffer.put_i32(*i));
        disk_manager.write(&1, &buffer).unwrap();

        let mut second_page = disk_manager.read(&1).unwrap();
        int_vals
            .iter()
            .for_each(|i| assert_eq!(second_page.get_i32(), *i));
    }
}
