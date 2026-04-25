use crate::error::ProtocolError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Connect = 0x01,
    Connack = 0x02,
    Ping = 0x10,
    Pong = 0x11,
    Pub = 0x20,
    Msg = 0x21,
    Sub = 0x30,
    Suback = 0x31,
    Unsub = 0x32,
    Unsuback = 0x33,
    Error = 0xE0,
}

impl FrameType {
    pub fn from_u8(value: u8) -> Result<Self, ProtocolError> {
        match value {
            0x01 => Ok(Self::Connect),
            0x02 => Ok(Self::Connack),
            0x10 => Ok(Self::Ping),
            0x11 => Ok(Self::Pong),
            0x20 => Ok(Self::Pub),
            0x21 => Ok(Self::Msg),
            0x30 => Ok(Self::Sub),
            0x31 => Ok(Self::Suback),
            0x32 => Ok(Self::Unsub),
            0x33 => Ok(Self::Unsuback),
            0xE0 => Ok(Self::Error),
            _ => Err(ProtocolError::UnknownFrameType(value)),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_types() {
        let types = [
            FrameType::Connect,
            FrameType::Connack,
            FrameType::Ping,
            FrameType::Pong,
            FrameType::Pub,
            FrameType::Msg,
            FrameType::Sub,
            FrameType::Suback,
            FrameType::Unsub,
            FrameType::Unsuback,
            FrameType::Error,
        ];
        for ft in types {
            assert_eq!(FrameType::from_u8(ft.as_u8()).unwrap(), ft);
        }
    }

    #[test]
    fn unknown_type_rejected() {
        assert!(FrameType::from_u8(0xFF).is_err());
    }
}
