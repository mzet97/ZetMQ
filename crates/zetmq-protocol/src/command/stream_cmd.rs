use bytes::Bytes;

/// CREATE_STREAM command payload:
/// [name_len(1) + name][max_msgs(8)][max_bytes(8)][max_age_secs(8)]
#[derive(Clone, Debug)]
pub struct CreateStreamCommand {
    pub name: String,
    pub max_msgs: u64,
    pub max_bytes: u64,
    pub max_age_secs: u64,
}

impl CreateStreamCommand {
    pub fn encode_payload(&self) -> Bytes {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(1 + name_bytes.len() + 24);
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.max_msgs.to_be_bytes());
        buf.extend_from_slice(&self.max_bytes.to_be_bytes());
        buf.extend_from_slice(&self.max_age_secs.to_be_bytes());
        Bytes::from(buf)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("empty payload".into());
        }
        let name_len = payload[0] as usize;
        if payload.len() < 1 + name_len + 24 {
            return Err("payload too short".into());
        }
        let name = String::from_utf8_lossy(&payload[1..1 + name_len]).to_string();
        let rest = &payload[1 + name_len..];
        let max_msgs = u64::from_be_bytes(rest[0..8].try_into().unwrap());
        let max_bytes = u64::from_be_bytes(rest[8..16].try_into().unwrap());
        let max_age_secs = u64::from_be_bytes(rest[16..24].try_into().unwrap());
        Ok(Self {
            name,
            max_msgs,
            max_bytes,
            max_age_secs,
        })
    }
}

/// DELETE_STREAM command payload:
/// [name_len(1) + name]
#[derive(Clone, Debug)]
pub struct DeleteStreamCommand {
    pub name: String,
}

impl DeleteStreamCommand {
    pub fn encode_payload(&self) -> Bytes {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(1 + name_bytes.len());
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        Bytes::from(buf)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("empty payload".into());
        }
        let name_len = payload[0] as usize;
        if payload.len() < 1 + name_len {
            return Err("payload too short".into());
        }
        let name = String::from_utf8_lossy(&payload[1..1 + name_len]).to_string();
        Ok(Self { name })
    }
}

/// ACK command payload:
/// [stream_name_len(1) + stream_name][sequence(8)]
#[derive(Clone, Debug)]
pub struct AckCommand {
    pub stream: String,
    pub sequence: u64,
}

impl AckCommand {
    pub fn encode_payload(&self) -> Bytes {
        let name_bytes = self.stream.as_bytes();
        let mut buf = Vec::with_capacity(1 + name_bytes.len() + 8);
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        Bytes::from(buf)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("empty payload".into());
        }
        let name_len = payload[0] as usize;
        if payload.len() < 1 + name_len + 8 {
            return Err("payload too short".into());
        }
        let stream = String::from_utf8_lossy(&payload[1..1 + name_len]).to_string();
        let sequence = u64::from_be_bytes(payload[1 + name_len..1 + name_len + 8].try_into().unwrap());
        Ok(Self { stream, sequence })
    }
}

/// NACK command payload:
/// [stream_name_len(1) + stream_name][sequence(8)]
#[derive(Clone, Debug)]
pub struct NackCommand {
    pub stream: String,
    pub sequence: u64,
}

impl NackCommand {
    pub fn encode_payload(&self) -> Bytes {
        let name_bytes = self.stream.as_bytes();
        let mut buf = Vec::with_capacity(1 + name_bytes.len() + 8);
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        Bytes::from(buf)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("empty payload".into());
        }
        let name_len = payload[0] as usize;
        if payload.len() < 1 + name_len + 8 {
            return Err("payload too short".into());
        }
        let stream = String::from_utf8_lossy(&payload[1..1 + name_len]).to_string();
        let sequence = u64::from_be_bytes(payload[1 + name_len..1 + name_len + 8].try_into().unwrap());
        Ok(Self { stream, sequence })
    }
}

/// STREAM_INFO response payload:
/// [name_len(1) + name][messages(8)][bytes(8)][first_seq(8)][last_seq(8)]
/// [max_msgs(8)][max_bytes(8)][max_age_secs(8)]
#[derive(Clone, Debug)]
pub struct StreamInfoResponse {
    pub name: String,
    pub messages: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub max_msgs: u64,
    pub max_bytes: u64,
    pub max_age_secs: u64,
}

impl StreamInfoResponse {
    pub fn encode_payload(&self) -> Bytes {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(1 + name_bytes.len() + 56);
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.messages.to_be_bytes());
        buf.extend_from_slice(&self.bytes.to_be_bytes());
        buf.extend_from_slice(&self.first_seq.to_be_bytes());
        buf.extend_from_slice(&self.last_seq.to_be_bytes());
        buf.extend_from_slice(&self.max_msgs.to_be_bytes());
        buf.extend_from_slice(&self.max_bytes.to_be_bytes());
        buf.extend_from_slice(&self.max_age_secs.to_be_bytes());
        Bytes::from(buf)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("empty payload".into());
        }
        let name_len = payload[0] as usize;
        if payload.len() < 1 + name_len + 56 {
            return Err("payload too short".into());
        }
        let name = String::from_utf8_lossy(&payload[1..1 + name_len]).to_string();
        let rest = &payload[1 + name_len..];
        Ok(Self {
            name,
            messages: u64::from_be_bytes(rest[0..8].try_into().unwrap()),
            bytes: u64::from_be_bytes(rest[8..16].try_into().unwrap()),
            first_seq: u64::from_be_bytes(rest[16..24].try_into().unwrap()),
            last_seq: u64::from_be_bytes(rest[24..32].try_into().unwrap()),
            max_msgs: u64::from_be_bytes(rest[32..40].try_into().unwrap()),
            max_bytes: u64::from_be_bytes(rest[40..48].try_into().unwrap()),
            max_age_secs: u64::from_be_bytes(rest[48..56].try_into().unwrap()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_stream_roundtrip() {
        let cmd = CreateStreamCommand {
            name: "orders".into(),
            max_msgs: 1000,
            max_bytes: 0,
            max_age_secs: 3600,
        };
        let payload = cmd.encode_payload();
        let decoded = CreateStreamCommand::decode(&payload).unwrap();
        assert_eq!(decoded.name, "orders");
        assert_eq!(decoded.max_msgs, 1000);
        assert_eq!(decoded.max_age_secs, 3600);
    }

    #[test]
    fn delete_stream_roundtrip() {
        let cmd = DeleteStreamCommand { name: "orders".into() };
        let payload = cmd.encode_payload();
        let decoded = DeleteStreamCommand::decode(&payload).unwrap();
        assert_eq!(decoded.name, "orders");
    }

    #[test]
    fn ack_roundtrip() {
        let cmd = AckCommand { stream: "orders".into(), sequence: 42 };
        let payload = cmd.encode_payload();
        let decoded = AckCommand::decode(&payload).unwrap();
        assert_eq!(decoded.stream, "orders");
        assert_eq!(decoded.sequence, 42);
    }

    #[test]
    fn stream_info_roundtrip() {
        let info = StreamInfoResponse {
            name: "orders".into(),
            messages: 100,
            bytes: 4096,
            first_seq: 1,
            last_seq: 100,
            max_msgs: 1000,
            max_bytes: 1_000_000,
            max_age_secs: 3600,
        };
        let payload = info.encode_payload();
        let decoded = StreamInfoResponse::decode(&payload).unwrap();
        assert_eq!(decoded.name, "orders");
        assert_eq!(decoded.messages, 100);
        assert_eq!(decoded.last_seq, 100);
    }
}
