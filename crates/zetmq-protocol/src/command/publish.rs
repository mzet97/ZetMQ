use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

pub type HeaderMap = HashMap<String, String>;

#[derive(Clone, Debug)]
pub struct PublishCommand {
    pub subject: Bytes,
    pub payload: Bytes,
    pub reply_to: Option<Bytes>,
    pub headers: Option<Arc<HeaderMap>>,
}
