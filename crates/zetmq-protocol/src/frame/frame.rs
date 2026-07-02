use bytes::{Bytes, BytesMut};

use crate::error::ProtocolError;
use crate::frame::frame_type::FrameType;
use crate::frame::header::{FrameHeader, FRAME_HEADER_SIZE};

#[derive(Clone, Debug)]
pub struct Frame {
    pub header: FrameHeader,
    pub headers: Bytes,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(frame_type: FrameType, correlation_id: u64) -> Self {
        let header = FrameHeader::new(frame_type.as_u8(), correlation_id);
        Self {
            header,
            headers: Bytes::new(),
            payload: Bytes::new(),
        }
    }

    pub fn with_payload(mut self, payload: Bytes) -> Self {
        self.header.payload_len = payload.len() as u32;
        self.payload = payload;
        self
    }

    pub fn with_headers(mut self, headers: Bytes) -> Self {
        self.header.header_len = headers.len() as u32;
        self.headers = headers;
        self
    }

    pub fn frame_type(&self) -> Result<FrameType, ProtocolError> {
        FrameType::from_u8(self.header.frame_type)
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.header.total_frame_size());
        self.encode_into(&mut buf);
        buf.freeze()
    }

    /// Encode directly into an existing buffer without allocation.
    pub fn encode_into(&self, buf: &mut BytesMut) {
        self.header.encode(buf);
        buf.extend_from_slice(&self.headers);
        buf.extend_from_slice(&self.payload);
    }

    pub fn decode_from(
        buf: &mut BytesMut,
        max_frame_size: usize,
    ) -> Result<Option<Self>, ProtocolError> {
        if buf.len() < FRAME_HEADER_SIZE {
            return Ok(None);
        }

        // Peek at lengths without consuming (header_len at bytes 14-17, payload_len at 18-21)
        let header_len = u32::from_be_bytes([buf[14], buf[15], buf[16], buf[17]]) as usize;
        let payload_len = u32::from_be_bytes([buf[18], buf[19], buf[20], buf[21]]) as usize;
        let total = FRAME_HEADER_SIZE + header_len + payload_len;

        if total > max_frame_size {
            return Err(ProtocolError::FrameTooLarge {
                size: total,
                limit: max_frame_size,
            });
        }

        if buf.len() < total {
            return Ok(None);
        }

        // Consume the whole frame in one split and slice the parts. This avoids
        // three separate split_to/freeze calls and their associated refcount ops.
        let frame_bytes = buf.split_to(total).freeze();

        let mut header_buf = frame_bytes.slice(0..FRAME_HEADER_SIZE);
        let header = FrameHeader::decode(&mut header_buf)?;

        let headers = if header_len > 0 {
            frame_bytes.slice(FRAME_HEADER_SIZE..FRAME_HEADER_SIZE + header_len)
        } else {
            Bytes::new()
        };
        let payload = if payload_len > 0 {
            frame_bytes.slice(FRAME_HEADER_SIZE + header_len..total)
        } else {
            Bytes::new()
        };

        Ok(Some(Self {
            header,
            headers,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_FRAME: usize = 2 * 1024 * 1024; // 2MB

    #[test]
    fn encode_decode_roundtrip() {
        let frame = Frame::new(FrameType::Pub, 1).with_payload(Bytes::from_static(b"hello world"));

        let encoded = frame.encode();
        let mut buf = BytesMut::from(&encoded[..]);

        let decoded = Frame::decode_from(&mut buf, MAX_FRAME).unwrap().unwrap();
        assert_eq!(decoded.frame_type().unwrap(), FrameType::Pub);
        assert_eq!(decoded.payload, Bytes::from_static(b"hello world"));
    }

    #[test]
    fn incomplete_frame_returns_none() {
        let mut buf = BytesMut::from(&b"\x5A\x4D"[..]);
        let result = Frame::decode_from(&mut buf, MAX_FRAME).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn frame_too_large() {
        let frame = Frame::new(FrameType::Pub, 1).with_payload(Bytes::from(vec![0u8; 100]));

        let encoded = frame.encode();
        let mut buf = BytesMut::from(&encoded[..]);

        let result = Frame::decode_from(&mut buf, 10); // limit = 10
        assert!(matches!(result, Err(ProtocolError::FrameTooLarge { .. })));
    }

    #[test]
    fn two_frames_in_buffer() {
        let f1 = Frame::new(FrameType::Ping, 1);
        let f2 = Frame::new(FrameType::Pong, 2);

        let mut buf = BytesMut::new();
        buf.extend_from_slice(&f1.encode());
        buf.extend_from_slice(&f2.encode());

        let decoded1 = Frame::decode_from(&mut buf, MAX_FRAME).unwrap().unwrap();
        assert_eq!(decoded1.frame_type().unwrap(), FrameType::Ping);

        let decoded2 = Frame::decode_from(&mut buf, MAX_FRAME).unwrap().unwrap();
        assert_eq!(decoded2.frame_type().unwrap(), FrameType::Pong);
    }
}
