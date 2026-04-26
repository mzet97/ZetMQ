use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use crate::error::ProtocolError;

/// Encode a header map into a bytes buffer.
/// Format: num_headers(2) + [key_len(2) + key + val_len(2) + val] × N
pub fn encode_headers(headers: &HashMap<String, String>, buf: &mut BytesMut) {
    buf.put_u16(headers.len() as u16);
    for (key, val) in headers {
        let key_bytes = key.as_bytes();
        let val_bytes = val.as_bytes();
        buf.put_u16(key_bytes.len() as u16);
        buf.extend_from_slice(key_bytes);
        buf.put_u16(val_bytes.len() as u16);
        buf.extend_from_slice(val_bytes);
    }
}

/// Decode a header map from a bytes slice.
pub fn decode_headers(data: &[u8]) -> Result<HashMap<String, String>, ProtocolError> {
    if data.len() < 2 {
        return Ok(HashMap::new());
    }
    let mut buf = data;
    let num_headers = buf.get_u16() as usize;
    let mut headers = HashMap::with_capacity(num_headers);
    for _ in 0..num_headers {
        if buf.remaining() < 2 {
            return Err(ProtocolError::DecodingError(
                "header key length truncated".into(),
            ));
        }
        let key_len = buf.get_u16() as usize;
        if buf.remaining() < key_len {
            return Err(ProtocolError::DecodingError(
                "header key data truncated".into(),
            ));
        }
        let key = String::from_utf8_lossy(&buf[..key_len]).to_string();
        buf.advance(key_len);

        if buf.remaining() < 2 {
            return Err(ProtocolError::DecodingError(
                "header value length truncated".into(),
            ));
        }
        let val_len = buf.get_u16() as usize;
        if buf.remaining() < val_len {
            return Err(ProtocolError::DecodingError(
                "header value data truncated".into(),
            ));
        }
        let val = String::from_utf8_lossy(&buf[..val_len]).to_string();
        buf.advance(val_len);

        headers.insert(key, val);
    }
    Ok(headers)
}

/// Calculate the encoded size of a header map.
pub fn encoded_headers_len(headers: &HashMap<String, String>) -> usize {
    let mut len = 2; // num_headers
    for (key, val) in headers {
        len += 2 + key.len() + 2 + val.len();
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty() {
        let headers = HashMap::new();
        let mut buf = BytesMut::new();
        encode_headers(&headers, &mut buf);
        let decoded = decode_headers(&buf).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn roundtrip_single() {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        let mut buf = BytesMut::new();
        encode_headers(&headers, &mut buf);
        let decoded = decode_headers(&buf).unwrap();
        assert_eq!(decoded.get("Content-Type").unwrap(), "application/json");
    }

    #[test]
    fn roundtrip_multiple() {
        let mut headers = HashMap::new();
        headers.insert("x-trace-id".to_string(), "abc123".to_string());
        headers.insert("x-source".to_string(), "order-service".to_string());
        let mut buf = BytesMut::new();
        encode_headers(&headers, &mut buf);
        let decoded = decode_headers(&buf).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.get("x-trace-id").unwrap(), "abc123");
        assert_eq!(decoded.get("x-source").unwrap(), "order-service");
    }

    #[test]
    fn decode_truncated_key() {
        let data: &[u8] = &[0, 1, 0, 5]; // 1 header, key_len=5 but no data
        let result = decode_headers(data);
        assert!(result.is_err());
    }
}
