use bytes::{Buf, BufMut, BytesMut};

use crate::error::ProtocolError;
use crate::version::CURRENT_VERSION;

pub const ZETMQ_MAGIC: u16 = 0x5A4D; // "ZM"
pub const FRAME_HEADER_SIZE: usize = 22; // 2+1+1+2+8+4+4

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub magic: u16,
    pub version: u8,
    pub frame_type: u8,
    pub flags: u16,
    pub correlation_id: u64,
    pub header_len: u32,
    pub payload_len: u32,
}

impl FrameHeader {
    pub fn new(frame_type: u8, correlation_id: u64) -> Self {
        Self {
            magic: ZETMQ_MAGIC,
            version: CURRENT_VERSION,
            frame_type,
            flags: 0,
            correlation_id,
            header_len: 0,
            payload_len: 0,
        }
    }

    pub fn with_payload_size(mut self, header_len: u32, payload_len: u32) -> Self {
        self.header_len = header_len;
        self.payload_len = payload_len;
        self
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u16(self.magic);
        buf.put_u8(self.version);
        buf.put_u8(self.frame_type);
        buf.put_u16(self.flags);
        buf.put_u64(self.correlation_id);
        buf.put_u32(self.header_len);
        buf.put_u32(self.payload_len);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < FRAME_HEADER_SIZE {
            return Err(ProtocolError::IncompleteFrame {
                needed: FRAME_HEADER_SIZE,
                available: buf.remaining(),
            });
        }

        let magic = buf.get_u16();
        if magic != ZETMQ_MAGIC {
            return Err(ProtocolError::InvalidMagic {
                expected: ZETMQ_MAGIC,
                got: magic,
            });
        }

        let version = buf.get_u8();
        if version != CURRENT_VERSION {
            return Err(ProtocolError::UnsupportedVersion(version));
        }

        let frame_type = buf.get_u8();
        let flags = buf.get_u16();
        let correlation_id = buf.get_u64();
        let header_len = buf.get_u32();
        let payload_len = buf.get_u32();

        Ok(Self {
            magic,
            version,
            frame_type,
            flags,
            correlation_id,
            header_len,
            payload_len,
        })
    }

    pub fn total_frame_size(&self) -> usize {
        FRAME_HEADER_SIZE + self.header_len as usize + self.payload_len as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let header = FrameHeader::new(0x20, 42).with_payload_size(0, 5);

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let decoded = FrameHeader::decode(&mut buf).unwrap();
        assert_eq!(decoded.magic, ZETMQ_MAGIC);
        assert_eq!(decoded.version, CURRENT_VERSION);
        assert_eq!(decoded.frame_type, 0x20);
        assert_eq!(decoded.correlation_id, 42);
        assert_eq!(decoded.payload_len, 5);
    }

    #[test]
    fn reject_incomplete_header() {
        let mut buf = BytesMut::from(&b"\x5A\x4D\x01"[..]);
        let result = FrameHeader::decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn reject_bad_magic() {
        let mut buf = BytesMut::new();
        buf.put_u16(0x0000); // wrong magic
        buf.put_u8(1);
        buf.put_u8(0x01);
        buf.put_u16(0);
        buf.put_u64(0);
        buf.put_u32(0);
        buf.put_u32(0);

        let result = FrameHeader::decode(&mut buf);
        assert!(matches!(result, Err(ProtocolError::InvalidMagic { .. })));
    }
}
