use crate::error::CoreError;

const MAX_QUEUE_GROUP_NAME: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueueGroupName(String);

impl QueueGroupName {
    pub fn new(name: &str) -> Result<Self, CoreError> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(CoreError::InvalidQueueGroupName(
                "queue group name cannot be empty".into(),
            ));
        }
        if trimmed.len() > MAX_QUEUE_GROUP_NAME {
            return Err(CoreError::InvalidQueueGroupName(format!(
                "name exceeds max length: {} > {}",
                trimmed.len(),
                MAX_QUEUE_GROUP_NAME
            )));
        }
        Ok(Self(trimmed.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueueGroupName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_name() {
        let name = QueueGroupName::new("workers").unwrap();
        assert_eq!(name.as_str(), "workers");
    }

    #[test]
    fn reject_empty() {
        assert!(QueueGroupName::new("").is_err());
        assert!(QueueGroupName::new("   ").is_err());
    }
}
