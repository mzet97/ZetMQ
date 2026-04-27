//! Append-only log segment for file-based persistence.
//!
//! Each segment is a file containing messages sequentially.
//! An in-memory index maps sequence numbers to file offsets for O(1) lookup.

use bytes::Bytes;
use std::io::{self, SeekFrom};
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// On-disk message record:
/// [seq: u64][timestamp: u64][subject_len: u16][subject][reply_len: u16][reply]
/// [payload_len: u32][payload]
use crate::error::StoreError;

/// Index entry: sequence -> file offset.
#[derive(Debug, Clone)]
struct IndexEntry {
    offset: u64,
    #[allow(dead_code)]
    timestamp: u64,
}

/// A single append-only segment file.
pub struct Segment {
    file: File,
    index: Vec<IndexEntry>,
    base_sequence: u64,
    bytes_written: u64,
    #[allow(dead_code)]
    path: std::path::PathBuf,
}

impl Segment {
    /// Open (or create) a segment at `path` with the given base sequence.
    pub async fn open(path: &Path, base_sequence: u64) -> Result<Self, StoreError> {
        let exists = path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(!exists)
            .open(path)
            .await?;

        let mut segment = Self {
            file,
            index: Vec::new(),
            base_sequence,
            bytes_written: 0,
            path: path.to_path_buf(),
        };

        if exists {
            segment.rebuild_index().await?;
        }

        Ok(segment)
    }

    /// Append a message and return the assigned sequence number.
    pub async fn append(
        &mut self,
        subject: &str,
        reply_to: Option<&str>,
        payload: &[u8],
        timestamp: u64,
    ) -> Result<u64, StoreError> {
        let seq = self.base_sequence + self.index.len() as u64;
        let offset = self.bytes_written;

        // Encode record
        let subject_bytes = subject.as_bytes();
        let reply_bytes = reply_to.map(|r| r.as_bytes());

        let record_size = 8 + 8 + 2 + subject_bytes.len()
            + 2
            + reply_bytes.map_or(0, |r| r.len())
            + 4
            + payload.len();

        let mut buf = Vec::with_capacity(record_size);
        buf.extend_from_slice(&seq.to_be_bytes());
        buf.extend_from_slice(&timestamp.to_be_bytes());
        buf.extend_from_slice(&(subject_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(subject_bytes);
        if let Some(reply) = &reply_bytes {
            buf.extend_from_slice(&(reply.len() as u16).to_be_bytes());
            buf.extend_from_slice(reply);
        } else {
            buf.extend_from_slice(&0u16.to_be_bytes());
        }
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(payload);

        self.file.write_all(&buf).await?;
        self.file.flush().await?;

        self.index.push(IndexEntry { offset, timestamp });
        self.bytes_written += buf.len() as u64;

        Ok(seq)
    }

    /// Read a message by sequence number. O(1) via index.
    pub async fn read(&mut self, sequence: u64) -> Result<Option<StoredRecord>, StoreError> {
        let idx = sequence.checked_sub(self.base_sequence);
        let Some(idx) = idx else {
            return Ok(None);
        };
        let entry = match self.index.get(idx as usize) {
            Some(e) => e,
            None => return Ok(None),
        };

        self.file.seek(SeekFrom::Start(entry.offset)).await?;

        // Read fixed header
        let seq = read_u64(&mut self.file).await?;
        let timestamp = read_u64(&mut self.file).await?;
        let subject = read_string(&mut self.file).await?;
        let reply_to = read_optional_string(&mut self.file).await?;
        let payload_len = read_u32(&mut self.file).await? as usize;
        let mut payload = vec![0u8; payload_len];
        self.file.read_exact(&mut payload).await?;

        Ok(Some(StoredRecord {
            sequence: seq,
            timestamp,
            subject,
            reply_to,
            payload: Bytes::from(payload),
        }))
    }

    /// Current message count in this segment.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Last sequence in this segment.
    pub fn last_sequence(&self) -> Option<u64> {
        if self.index.is_empty() {
            None
        } else {
            Some(self.base_sequence + self.index.len() as u64 - 1)
        }
    }

    /// Rebuild index from file by scanning records.
    async fn rebuild_index(&mut self) -> Result<(), StoreError> {
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut offset: u64 = 0;

        loop {
            // Try to read seq + timestamp
            let mut header = [0u8; 16];
            if self.file.read_exact(&mut header).await.is_err() {
                break;
            }

            let timestamp = u64::from_be_bytes(header[8..16].try_into().unwrap());

            // Read subject
            let mut slen = [0u8; 2];
            if self.file.read_exact(&mut slen).await.is_err() {
                break;
            }
            let subject_len = u16::from_be_bytes(slen) as usize;
            if self.file.seek(SeekFrom::Current(subject_len as i64)).await.is_err() {
                break;
            }

            // Read reply
            let mut rlen = [0u8; 2];
            if self.file.read_exact(&mut rlen).await.is_err() {
                break;
            }
            let reply_len = u16::from_be_bytes(rlen) as i64;
            if reply_len > 0 && self.file.seek(SeekFrom::Current(reply_len)).await.is_err() {
                break;
            }

            // Read payload length
            let mut plen = [0u8; 4];
            if self.file.read_exact(&mut plen).await.is_err() {
                break;
            }
            let payload_len = u32::from_be_bytes(plen) as i64;

            if self.file.seek(SeekFrom::Current(payload_len)).await.is_err() {
                break;
            }

            self.index.push(IndexEntry { offset, timestamp });

            // Total record: 8(seq) + 8(ts) + 2(slen) + subject + 2(rlen) + reply + 4(plen) + payload
            let record_size = 8 + 8 + 2 + subject_len as i64 + 2 + reply_len + 4 + payload_len;
            offset += record_size as u64;
        }

        self.bytes_written = offset;
        Ok(())
    }
}

/// A record read from a segment file.
#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub sequence: u64,
    pub timestamp: u64,
    pub subject: String,
    pub reply_to: Option<String>,
    pub payload: Bytes,
}

async fn read_u64(file: &mut File) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).await?;
    Ok(u64::from_be_bytes(buf))
}

async fn read_u32(file: &mut File) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf).await?;
    Ok(u32::from_be_bytes(buf))
}

async fn read_string(file: &mut File) -> io::Result<String> {
    let mut len_buf = [0u8; 2];
    file.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).to_string())
}

async fn read_optional_string(file: &mut File) -> io::Result<Option<String>> {
    let s = read_string(file).await?;
    if s.is_empty() {
        Ok(None)
    } else {
        Ok(Some(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn append_and_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg01.log");
        let mut seg = Segment::open(&path, 1).await.unwrap();

        let ts = 1000u64;
        let seq = seg
            .append("test.subject", None, b"hello", ts)
            .await
            .unwrap();
        assert_eq!(seq, 1);

        let record = seg.read(1).await.unwrap().unwrap();
        assert_eq!(record.sequence, 1);
        assert_eq!(record.subject, "test.subject");
        assert_eq!(&record.payload[..], b"hello");
    }

    #[tokio::test]
    async fn multiple_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg02.log");
        let mut seg = Segment::open(&path, 1).await.unwrap();

        for i in 0u8..10 {
            seg.append("s", None, &[i], 1000 + i as u64)
                .await
                .unwrap();
        }

        assert_eq!(seg.len(), 10);
        let r5 = seg.read(5).await.unwrap().unwrap();
        assert_eq!(&r5.payload[..], &[4]);
    }

    #[tokio::test]
    async fn rebuild_index_on_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg03.log");

        // Write some data
        {
            let mut seg = Segment::open(&path, 1).await.unwrap();
            for i in 0u8..5 {
                seg.append("s", Some("reply"), &[i], 2000)
                    .await
                    .unwrap();
            }
        }

        // Reopen and verify
        let mut seg = Segment::open(&path, 1).await.unwrap();
        assert_eq!(seg.len(), 5);
        let r = seg.read(3).await.unwrap().unwrap();
        assert_eq!(&r.payload[..], &[2]);
        assert_eq!(r.reply_to.as_deref(), Some("reply"));
    }
}
