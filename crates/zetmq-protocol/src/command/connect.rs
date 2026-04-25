#[derive(Clone, Debug)]
pub struct ConnectCommand {
    pub client_name: Option<String>,
    pub protocol_version: u8,
}

impl ConnectCommand {
    pub fn new(protocol_version: u8) -> Self {
        Self {
            client_name: None,
            protocol_version,
        }
    }
}
