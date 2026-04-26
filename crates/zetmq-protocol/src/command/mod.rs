pub mod connect;
pub mod ping;
pub mod publish;
pub mod subscribe;
pub mod unsubscribe;

pub use connect::{AuthInfo, ConnectCommand};
pub use ping::PingCommand;
pub use publish::PublishCommand;
pub use subscribe::SubscribeCommand;
pub use unsubscribe::UnsubscribeCommand;

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::frame::{Frame, FrameType};

#[derive(Clone, Debug)]
pub enum BrokerCommand {
    Connect(ConnectCommand),
    Publish(PublishCommand),
    Subscribe(SubscribeCommand),
    Unsubscribe(UnsubscribeCommand),
    Ping(PingCommand),
}

impl BrokerCommand {
    pub fn from_frame(frame: Frame) -> Result<Self, ProtocolError> {
        let ft = FrameType::from_u8(frame.header.frame_type)?;
        match ft {
            FrameType::Connect => {
                let mut cmd = ConnectCommand::new(frame.header.version);
                if !frame.payload.is_empty() {
                    let auth_type = frame.payload[0];
                    let auth_data = &frame.payload[1..];
                    match AuthInfo::decode(auth_type, auth_data) {
                        Ok(auth) => cmd.auth = auth,
                        Err(e) => {
                            return Err(ProtocolError::DecodingError(format!("invalid auth: {e}")))
                        }
                    }
                }
                Ok(Self::Connect(cmd))
            }
            FrameType::Pub => {
                // Payload format: subject_len(2) + subject + [reply_len(2) + reply] + payload
                // Headers: in frame.headers section (encoded HeaderMap)
                let payload = &frame.payload;
                if payload.len() < 2 {
                    return Err(ProtocolError::DecodingError("PUB frame too short".into()));
                }
                let subj_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                if payload.len() < 2 + subj_len {
                    return Err(ProtocolError::DecodingError("PUB subject truncated".into()));
                }

                // Zero-copy: slice subject bytes directly from frame payload
                let subject = frame.payload.slice(2..2 + subj_len);

                let rest = &payload[2 + subj_len..];
                let (reply_to, data_start) = if rest.len() >= 2 {
                    let reply_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
                    if rest.len() >= 2 + reply_len && reply_len > 0 {
                        let reply_offset = 2 + subj_len + 2;
                        let reply = frame.payload.slice(reply_offset..reply_offset + reply_len);
                        (Some(reply), 2 + reply_len)
                    } else {
                        (None, 2)
                    }
                } else {
                    (None, 0)
                };

                // Zero-copy: slice payload data directly from frame
                let payload_offset = 2 + subj_len + data_start;
                let msg_payload = if payload_offset < frame.payload.len() {
                    frame.payload.slice(payload_offset..)
                } else {
                    Bytes::new()
                };

                // Parse headers from frame header section
                let headers = if !frame.headers.is_empty() {
                    Some(crate::headers::decode_headers(&frame.headers)?)
                } else {
                    None
                };

                Ok(Self::Publish(PublishCommand {
                    subject,
                    payload: msg_payload,
                    reply_to,
                    headers,
                }))
            }
            FrameType::Sub => {
                let payload = &frame.payload;
                if payload.is_empty() {
                    return Err(ProtocolError::DecodingError("SUB frame empty".into()));
                }
                let pattern_len = payload[0] as usize;
                if payload.len() < 1 + pattern_len {
                    return Err(ProtocolError::DecodingError("SUB pattern truncated".into()));
                }
                let subject_pattern =
                    String::from_utf8_lossy(&payload[1..1 + pattern_len]).to_string();
                let queue_group = if payload.len() > 1 + pattern_len + 1 {
                    let qg_len = payload[1 + pattern_len] as usize;
                    if payload.len() >= 2 + pattern_len + qg_len && qg_len > 0 {
                        Some(
                            String::from_utf8_lossy(
                                &payload[2 + pattern_len..2 + pattern_len + qg_len],
                            )
                            .to_string(),
                        )
                    } else {
                        None
                    }
                } else {
                    None
                };
                Ok(Self::Subscribe(SubscribeCommand {
                    subject_pattern,
                    queue_group,
                }))
            }
            FrameType::Unsub => {
                if frame.payload.len() < 8 {
                    return Err(ProtocolError::DecodingError("UNSUB frame too short".into()));
                }
                let id = u64::from_be_bytes([
                    frame.payload[0],
                    frame.payload[1],
                    frame.payload[2],
                    frame.payload[3],
                    frame.payload[4],
                    frame.payload[5],
                    frame.payload[6],
                    frame.payload[7],
                ]);
                Ok(Self::Unsubscribe(UnsubscribeCommand {
                    subscription_id: id,
                }))
            }
            FrameType::Ping => Ok(Self::Ping(PingCommand)),
            _ => Err(ProtocolError::DecodingError(format!(
                "unexpected frame type for command: {:?}",
                ft
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_pub_frame(subject: &str, payload: &[u8]) -> Frame {
        let mut data = Vec::new();
        let subj_bytes = subject.as_bytes();
        data.extend_from_slice(&(subj_bytes.len() as u16).to_be_bytes());
        data.extend_from_slice(subj_bytes);
        data.extend_from_slice(&(0u16.to_be_bytes())); // no reply_to
        data.extend_from_slice(payload);
        Frame::new(FrameType::Pub, 1).with_payload(Bytes::from(data))
    }

    fn make_sub_frame(pattern: &str, queue_group: Option<&str>) -> Frame {
        let mut data = Vec::new();
        let pat_bytes = pattern.as_bytes();
        data.push(pat_bytes.len() as u8);
        data.extend_from_slice(pat_bytes);
        if let Some(qg) = queue_group {
            let qg_bytes = qg.as_bytes();
            data.push(qg_bytes.len() as u8);
            data.extend_from_slice(qg_bytes);
        }
        Frame::new(FrameType::Sub, 2).with_payload(Bytes::from(data))
    }

    #[test]
    fn parse_connect_no_auth() {
        let frame = Frame::new(FrameType::Connect, 1);
        let cmd = BrokerCommand::from_frame(frame).unwrap();
        match cmd {
            BrokerCommand::Connect(c) => {
                assert_eq!(c.protocol_version, 1);
                assert_eq!(c.auth, AuthInfo::None);
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_connect_with_token() {
        let mut payload = vec![1u8]; // auth_type = token
        let token = b"secret-token";
        payload.extend_from_slice(&(token.len() as u16).to_be_bytes());
        payload.extend_from_slice(token);
        let frame = Frame::new(FrameType::Connect, 1).with_payload(Bytes::from(payload));
        let cmd = BrokerCommand::from_frame(frame).unwrap();
        match cmd {
            BrokerCommand::Connect(c) => {
                assert_eq!(c.auth, AuthInfo::Token("secret-token".into()));
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_connect_with_userpass() {
        let mut payload = vec![2u8]; // auth_type = userpass
        let user = b"admin";
        let pass = b"password123";
        payload.extend_from_slice(&(user.len() as u16).to_be_bytes());
        payload.extend_from_slice(user);
        payload.extend_from_slice(&(pass.len() as u16).to_be_bytes());
        payload.extend_from_slice(pass);
        let frame = Frame::new(FrameType::Connect, 1).with_payload(Bytes::from(payload));
        let cmd = BrokerCommand::from_frame(frame).unwrap();
        match cmd {
            BrokerCommand::Connect(c) => {
                assert_eq!(
                    c.auth,
                    AuthInfo::UserPass {
                        username: "admin".into(),
                        password: "password123".into(),
                    }
                );
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn parse_pub() {
        let frame = make_pub_frame("orders.created", b"hello");
        let cmd = BrokerCommand::from_frame(frame).unwrap();
        match cmd {
            BrokerCommand::Publish(p) => {
                assert_eq!(&p.subject[..], b"orders.created");
                assert_eq!(&p.payload[..], b"hello");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn parse_sub() {
        let frame = make_sub_frame("orders.*", Some("workers"));
        let cmd = BrokerCommand::from_frame(frame).unwrap();
        match cmd {
            BrokerCommand::Subscribe(s) => {
                assert_eq!(s.subject_pattern, "orders.*");
                assert_eq!(s.queue_group.as_deref(), Some("workers"));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn parse_ping() {
        let frame = Frame::new(FrameType::Ping, 0);
        let cmd = BrokerCommand::from_frame(frame).unwrap();
        assert!(matches!(cmd, BrokerCommand::Ping(_)));
    }
}
