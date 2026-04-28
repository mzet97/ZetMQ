pub const PROTOCOL_V1: u8 = 1;
pub const CURRENT_VERSION: u8 = PROTOCOL_V1;

pub fn is_supported(version: u8) -> bool {
    version == PROTOCOL_V1
}
