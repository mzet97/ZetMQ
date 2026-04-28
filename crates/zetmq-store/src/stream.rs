use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Configuration for a stream's retention policy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Maximum number of messages retained. 0 = unlimited.
    pub max_msgs: u64,
    /// Maximum total bytes retained. 0 = unlimited.
    pub max_bytes: u64,
    /// Maximum age per message in seconds. 0 = unlimited.
    pub max_age_secs: u64,
}

impl StreamConfig {
    pub fn with_max_msgs(mut self, n: u64) -> Self {
        self.max_msgs = n;
        self
    }

    pub fn with_max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = n;
        self
    }

    pub fn with_max_age_secs(mut self, secs: u64) -> Self {
        self.max_age_secs = secs;
        self
    }
}

/// A stored message with metadata.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    /// Monotonically increasing sequence number (1-based).
    pub sequence: u64,
    /// Timestamp when the message was stored (millis since epoch).
    pub timestamp: u64,
    /// Subject the message was published to.
    pub subject: String,
    /// Optional reply-to subject.
    pub reply_to: Option<String>,
    /// Message payload.
    pub payload: Bytes,
    /// Optional headers.
    pub headers: Option<Vec<(String, String)>>,
}

/// Runtime info about a stream.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: String,
    pub config: StreamConfig,
    pub state: StreamState,
}

/// Current state counters for a stream.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamState {
    /// Total messages in the stream.
    pub messages: u64,
    /// Total bytes in the stream.
    pub bytes: u64,
    /// First sequence number still retained.
    pub first_seq: u64,
    /// Last sequence number written.
    pub last_seq: u64,
}

/// A single stream: named, ordered collection of messages with retention config.
pub struct Stream {
    name: String,
    config: StreamConfig,
    state: StreamState,
    messages: Vec<StoredMessage>,
}

impl Stream {
    pub fn new(name: String, config: StreamConfig) -> Self {
        Self {
            name,
            config,
            state: StreamState {
                messages: 0,
                bytes: 0,
                first_seq: 1,
                last_seq: 0,
            },
            messages: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn config(&self) -> &StreamConfig {
        &self.config
    }

    pub fn state(&self) -> &StreamState {
        &self.state
    }

    pub fn info(&self) -> StreamInfo {
        StreamInfo {
            name: self.name.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
        }
    }

    /// Append a message. Returns the assigned sequence number.
    pub fn store(
        &mut self,
        subject: String,
        reply_to: Option<String>,
        payload: Bytes,
        headers: Option<Vec<(String, String)>>,
    ) -> u64 {
        let seq = self.state.last_seq + 1;
        let timestamp = now_millis();
        let msg_bytes = payload.len() as u64;

        let msg = StoredMessage {
            sequence: seq,
            timestamp,
            subject,
            reply_to,
            payload,
            headers,
        };

        self.messages.push(msg);
        self.state.last_seq = seq;
        self.state.messages += 1;
        self.state.bytes += msg_bytes;

        self.apply_retention();
        seq
    }

    /// Read a message by sequence number. O(1) via index arithmetic.
    pub fn read(&self, sequence: u64) -> Option<&StoredMessage> {
        if sequence < self.state.first_seq || sequence > self.state.last_seq {
            return None;
        }
        let idx = (sequence - self.state.first_seq) as usize;
        self.messages.get(idx)
    }

    /// Read a range of messages [start, end] inclusive.
    pub fn read_range(&self, start: u64, end: u64) -> Vec<&StoredMessage> {
        let s = start.max(self.state.first_seq);
        let e = end.min(self.state.last_seq);
        if s > e {
            return Vec::new();
        }
        let start_idx = (s - self.state.first_seq) as usize;
        let end_idx = (e - self.state.first_seq) as usize;
        self.messages[start_idx..=end_idx].iter().collect()
    }

    /// Apply retention policies, removing oldest messages that exceed limits.
    fn apply_retention(&mut self) {
        let now = now_millis();

        // Age-based retention
        if self.config.max_age_secs > 0 {
            let cutoff = now - (self.config.max_age_secs * 1000);
            while let Some(front) = self.messages.first() {
                if front.timestamp < cutoff {
                    self.remove_front();
                } else {
                    break;
                }
            }
        }

        // Count-based retention
        if self.config.max_msgs > 0 {
            while self.messages.len() as u64 > self.config.max_msgs {
                self.remove_front();
            }
        }

        // Byte-based retention
        if self.config.max_bytes > 0 {
            while self.state.bytes > self.config.max_bytes && self.messages.len() > 1 {
                self.remove_front();
            }
        }
    }

    fn remove_front(&mut self) {
        if !self.messages.is_empty() {
            let msg = self.messages.remove(0);
            self.state.bytes -= msg.payload.len() as u64;
            self.state.messages -= 1;
            self.state.first_seq += 1;
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_and_read() {
        let mut stream = Stream::new("test".into(), StreamConfig::default());
        let seq = stream.store("foo".into(), None, Bytes::from("hello"), None);
        assert_eq!(seq, 1);

        let msg = stream.read(1).unwrap();
        assert_eq!(msg.subject, "foo");
        assert_eq!(&msg.payload[..], b"hello");
    }

    #[test]
    fn read_range() {
        let mut stream = Stream::new("test".into(), StreamConfig::default());
        for i in 0..5 {
            stream.store("s".into(), None, Bytes::from(vec![i]), None);
        }
        let range: Vec<_> = stream.read_range(2, 4);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].sequence, 2);
        assert_eq!(range[2].sequence, 4);
    }

    #[test]
    fn max_msgs_retention() {
        let mut stream = Stream::new("test".into(), StreamConfig::default().with_max_msgs(3));
        for i in 1..=5u8 {
            stream.store("s".into(), None, Bytes::from(vec![i]), None);
        }
        assert_eq!(stream.state.messages, 3);
        assert_eq!(stream.state.first_seq, 3);
        assert_eq!(stream.state.last_seq, 5);
        assert!(stream.read(1).is_none());
        assert!(stream.read(3).is_some());
    }

    #[test]
    fn max_bytes_retention() {
        let mut stream = Stream::new("test".into(), StreamConfig::default().with_max_bytes(10));
        stream.store("s".into(), None, Bytes::from(vec![0; 4]), None);
        stream.store("s".into(), None, Bytes::from(vec![0; 4]), None);
        stream.store("s".into(), None, Bytes::from(vec![0; 4]), None);
        // 3x4 = 12 bytes, max 10 → evict oldest until <= 10
        assert!(stream.state.bytes <= 10);
    }
}
