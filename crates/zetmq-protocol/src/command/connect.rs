use bytes::Bytes;

/// Authentication credentials sent in the CONNECT frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthInfo {
    None,
    Token(String),
    UserPass { username: String, password: String },
}

impl AuthInfo {
    /// Auth type byte written into CONNECT payload.
    pub fn type_byte(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Token(_) => 1,
            Self::UserPass { .. } => 2,
        }
    }

    /// Encode auth fields into a byte vector (caller prepends the type_byte).
    pub fn encode_fields(&self) -> Vec<u8> {
        match self {
            Self::None => Vec::new(),
            Self::Token(token) => {
                let bytes = token.as_bytes();
                let mut buf = Vec::with_capacity(2 + bytes.len());
                buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(bytes);
                buf
            }
            Self::UserPass { username, password } => {
                let u_bytes = username.as_bytes();
                let p_bytes = password.as_bytes();
                let mut buf = Vec::with_capacity(2 + u_bytes.len() + 2 + p_bytes.len());
                buf.extend_from_slice(&(u_bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(u_bytes);
                buf.extend_from_slice(&(p_bytes.len() as u16).to_be_bytes());
                buf.extend_from_slice(p_bytes);
                buf
            }
        }
    }

    /// Decode auth fields from CONNECT payload.
    /// `data` is the payload AFTER the type byte.
    pub fn decode(auth_type: u8, data: &[u8]) -> Result<Self, String> {
        match auth_type {
            0 => Ok(Self::None),
            1 => {
                if data.len() < 2 {
                    return Err("token auth: payload too short".into());
                }
                let len = u16::from_be_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + len {
                    return Err("token auth: token truncated".into());
                }
                let token = String::from_utf8_lossy(&data[2..2 + len]).to_string();
                Ok(Self::Token(token))
            }
            2 => {
                if data.len() < 2 {
                    return Err("userpass auth: payload too short".into());
                }
                let u_len = u16::from_be_bytes([data[0], data[1]]) as usize;
                if data.len() < 2 + u_len + 2 {
                    return Err("userpass auth: username truncated".into());
                }
                let username = String::from_utf8_lossy(&data[2..2 + u_len]).to_string();
                let p_len = u16::from_be_bytes([data[2 + u_len], data[3 + u_len]]) as usize;
                if data.len() < 4 + u_len + p_len {
                    return Err("userpass auth: password truncated".into());
                }
                let password =
                    String::from_utf8_lossy(&data[4 + u_len..4 + u_len + p_len]).to_string();
                Ok(Self::UserPass { username, password })
            }
            other => Err(format!("unknown auth type: {other}")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConnectCommand {
    pub client_name: Option<String>,
    pub protocol_version: u8,
    pub auth: AuthInfo,
}

impl ConnectCommand {
    pub fn new(protocol_version: u8) -> Self {
        Self {
            client_name: None,
            protocol_version,
            auth: AuthInfo::None,
        }
    }

    /// Encode the CONNECT payload including auth info.
    pub fn encode_payload(&self) -> Option<Bytes> {
        match &self.auth {
            AuthInfo::None => None,
            auth => {
                let mut buf = Vec::new();
                buf.push(auth.type_byte());
                buf.extend_from_slice(&auth.encode_fields());
                Some(Bytes::from(buf))
            }
        }
    }
}
