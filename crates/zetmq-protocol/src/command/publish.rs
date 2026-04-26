use std::collections::HashMap;

use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct PublishCommand {
    pub subject: Bytes,
    pub payload: Bytes,
    pub reply_to: Option<Bytes>,
    pub headers: Option<HashMap<String, String>>,
}
