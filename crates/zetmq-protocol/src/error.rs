use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("invalid magic bytes: expected 0x{expected:04X}, got 0x{got:04X}")]
    InvalidMagic { expected: u16, got: u16 },

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("unknown frame type: {0}")]
    UnknownFrameType(u8),

    #[error("frame too large: {size} > {limit}")]
    FrameTooLarge { size: usize, limit: usize },

    #[error("payload too large: {size} > {limit}")]
    PayloadTooLarge { size: usize, limit: usize },

    #[error("incomplete frame: need {needed} bytes, have {available}")]
    IncompleteFrame { needed: usize, available: usize },

    #[error("invalid header length: {0}")]
    InvalidHeaderLength(usize),

    #[error("encoding error: {0}")]
    EncodingError(String),

    #[error("decoding error: {0}")]
    DecodingError(String),
}
