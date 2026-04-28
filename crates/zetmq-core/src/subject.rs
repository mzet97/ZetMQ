use std::sync::Arc;

use crate::error::CoreError;

const MAX_SUBJECT_LEN: usize = 512;
const MAX_TOKENS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subject {
    raw: Arc<str>,
    tokens: Arc<[String]>,
}

impl Subject {
    pub fn parse(input: &str) -> Result<Self, CoreError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(CoreError::InvalidSubject("subject cannot be empty".into()));
        }

        if trimmed.len() > MAX_SUBJECT_LEN {
            return Err(CoreError::SubjectTooLong {
                len: trimmed.len(),
                limit: MAX_SUBJECT_LEN,
            });
        }

        if trimmed.contains('*') || trimmed.contains('>') {
            return Err(CoreError::InvalidSubject(
                "publish subject cannot contain wildcards".into(),
            ));
        }

        let tokens: Vec<String> = trimmed.split('.').map(String::from).collect();

        if tokens.len() > MAX_TOKENS {
            return Err(CoreError::InvalidSubject(format!(
                "subject exceeds max tokens: {} > {}",
                tokens.len(),
                MAX_TOKENS
            )));
        }

        for token in &tokens {
            if token.is_empty() {
                return Err(CoreError::InvalidSubject(
                    "subject contains empty token".into(),
                ));
            }
        }

        Ok(Self {
            raw: Arc::from(trimmed),
            tokens: Arc::from(tokens),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn tokens(&self) -> &[String] {
        &self.tokens
    }

    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_simple_subject() {
        let s = Subject::parse("orders.created").unwrap();
        assert_eq!(s.as_str(), "orders.created");
        assert_eq!(s.tokens(), &["orders", "created"]);
    }

    #[test]
    fn valid_multi_token_subject() {
        let s = Subject::parse("metrics.cpu.host01").unwrap();
        assert_eq!(s.token_count(), 3);
    }

    #[test]
    fn reject_empty() {
        assert!(Subject::parse("").is_err());
        assert!(Subject::parse("   ").is_err());
    }

    #[test]
    fn reject_empty_token() {
        assert!(Subject::parse("orders..created").is_err());
    }

    #[test]
    fn reject_leading_dot() {
        assert!(Subject::parse(".orders").is_err());
    }

    #[test]
    fn reject_trailing_dot() {
        assert!(Subject::parse("orders.").is_err());
    }

    #[test]
    fn reject_wildcard_star() {
        assert!(Subject::parse("orders.*").is_err());
    }

    #[test]
    fn reject_wildcard_gt() {
        assert!(Subject::parse("orders.>").is_err());
    }

    #[test]
    fn subject_equality() {
        let a = Subject::parse("orders.created").unwrap();
        let b = Subject::parse("orders.created").unwrap();
        assert_eq!(a, b);
    }
}
