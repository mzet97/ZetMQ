use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct PublishCommand {
    pub subject: String,
    pub payload: Bytes,
    pub reply_to: Option<String>,
}
