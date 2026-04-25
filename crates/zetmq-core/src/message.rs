use bytes::Bytes;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::id::MessageId;
use crate::subject::Subject;

pub type HeaderMap = HashMap<String, String>;

#[derive(Clone, Debug)]
pub struct Message {
    pub id: MessageId,
    pub subject: Subject,
    pub payload: Bytes,
    pub headers: HeaderMap,
    pub reply_to: Option<Subject>,
    pub timestamp_ns: u64,
}

impl Message {
    pub fn new(subject: Subject, payload: Bytes) -> Self {
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        Self {
            id: MessageId::new(0),
            subject,
            payload,
            headers: HeaderMap::new(),
            reply_to: None,
            timestamp_ns,
        }
    }

    pub fn with_id(mut self, id: MessageId) -> Self {
        self.id = id;
        self
    }

    pub fn with_reply_to(mut self, subject: Subject) -> Self {
        self.reply_to = Some(subject);
        self
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_subject(s: &str) -> Subject {
        Subject::parse(s).unwrap()
    }

    #[test]
    fn create_message() {
        let subj = test_subject("orders.created");
        let payload = Bytes::from_static(b"hello");
        let msg = Message::new(subj.clone(), payload.clone());
        assert_eq!(msg.subject, subj);
        assert_eq!(msg.payload, payload);
        assert!(msg.reply_to.is_none());
        assert!(msg.timestamp_ns > 0);
    }

    #[test]
    fn clone_preserves_payload() {
        let subj = test_subject("test");
        let payload = Bytes::from(vec![0u8; 1024]);
        let msg = Message::new(subj, payload);
        let cloned = msg.clone();
        assert_eq!(msg.payload.len(), cloned.payload.len());
    }

    #[test]
    fn with_reply_to() {
        let subj = test_subject("req");
        let reply = test_subject("_INBOX.123");
        let msg = Message::new(subj, Bytes::new()).with_reply_to(reply.clone());
        assert_eq!(msg.reply_to.unwrap(), reply);
    }
}
