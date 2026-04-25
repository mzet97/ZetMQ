#[derive(Clone, Debug)]
pub struct SubscribeCommand {
    pub subject_pattern: String,
    pub queue_group: Option<String>,
}
