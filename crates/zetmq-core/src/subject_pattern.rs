use crate::error::CoreError;
use crate::subject::Subject;

const MAX_PATTERN_LEN: usize = 512;
const MAX_TOKENS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubjectPattern {
    raw: String,
    tokens: Vec<PatternToken>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PatternToken {
    Literal(String),
    SingleWildcard,  // *
    MultiWildcard,   // >
}

impl SubjectPattern {
    pub fn parse(input: &str) -> Result<Self, CoreError> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(CoreError::InvalidSubjectPattern(
                "pattern cannot be empty".into(),
            ));
        }

        if trimmed.len() > MAX_PATTERN_LEN {
            return Err(CoreError::InvalidSubjectPattern(format!(
                "pattern exceeds max length: {} > {}",
                trimmed.len(),
                MAX_PATTERN_LEN
            )));
        }

        let raw_tokens: Vec<&str> = trimmed.split('.').collect();

        if raw_tokens.len() > MAX_TOKENS {
            return Err(CoreError::InvalidSubjectPattern(format!(
                "pattern exceeds max tokens: {} > {}",
                raw_tokens.len(),
                MAX_TOKENS
            )));
        }

        let mut tokens = Vec::with_capacity(raw_tokens.len());

        for (i, token_str) in raw_tokens.iter().enumerate() {
            if token_str.is_empty() {
                return Err(CoreError::InvalidSubjectPattern(
                    "pattern contains empty token".into(),
                ));
            }

            match *token_str {
                "*" => {
                    tokens.push(PatternToken::SingleWildcard);
                }
                ">" => {
                    if i != raw_tokens.len() - 1 {
                        return Err(CoreError::InvalidSubjectPattern(
                            "'>' wildcard must be the last token".into(),
                        ));
                    }
                    tokens.push(PatternToken::MultiWildcard);
                }
                literal => {
                    tokens.push(PatternToken::Literal(literal.to_string()));
                }
            }
        }

        Ok(Self {
            raw: trimmed.to_string(),
            tokens,
        })
    }

    pub fn matches(&self, subject: &Subject) -> bool {
        let subject_tokens = subject.tokens();
        let pattern_tokens = &self.tokens;

        let mut si = 0;
        let mut pi = 0;

        while pi < pattern_tokens.len() && si < subject_tokens.len() {
            match &pattern_tokens[pi] {
                PatternToken::Literal(lit) => {
                    if subject_tokens[si] != *lit {
                        return false;
                    }
                    si += 1;
                    pi += 1;
                }
                PatternToken::SingleWildcard => {
                    si += 1;
                    pi += 1;
                }
                PatternToken::MultiWildcard => {
                    return subject_tokens.len() > si;
                }
            }
        }

        pi == pattern_tokens.len() && si == subject_tokens.len()
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn tokens(&self) -> &[PatternToken] {
        &self.tokens
    }
}

impl std::fmt::Display for SubjectPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(s: &str) -> Subject { Subject::parse(s).unwrap() }
    fn pattern(s: &str) -> SubjectPattern { SubjectPattern::parse(s).unwrap() }

    #[test]
    fn valid_exact_pattern() {
        let p = pattern("orders.created");
        assert_eq!(p.as_str(), "orders.created");
    }

    #[test]
    fn valid_single_wildcard() {
        let p = pattern("orders.*");
        assert_eq!(p.tokens().len(), 2);
    }

    #[test]
    fn valid_multi_wildcard() {
        let p = pattern("orders.>");
        assert_eq!(p.tokens().len(), 2);
    }

    #[test]
    fn reject_empty() { assert!(SubjectPattern::parse("").is_err()); }

    #[test]
    fn reject_empty_token() {
        assert!(SubjectPattern::parse("orders..created").is_err());
    }

    #[test]
    fn reject_gt_in_middle() {
        assert!(SubjectPattern::parse("orders.>.created").is_err());
    }

    #[test]
    fn exact_match() {
        assert!(pattern("orders.created").matches(&subject("orders.created")));
    }

    #[test]
    fn exact_no_match() {
        assert!(!pattern("orders.created").matches(&subject("orders.cancelled")));
    }

    #[test]
    fn star_matches_one_token() {
        let p = pattern("orders.*");
        assert!(p.matches(&subject("orders.created")));
        assert!(p.matches(&subject("orders.cancelled")));
    }

    #[test]
    fn star_no_match_multiple() {
        assert!(!pattern("orders.*").matches(&subject("orders.created.high")));
    }

    #[test]
    fn star_no_match_zero() {
        assert!(!pattern("orders.*").matches(&subject("orders")));
    }

    #[test]
    fn gt_matches_one_token() {
        assert!(pattern("orders.>").matches(&subject("orders.created")));
    }

    #[test]
    fn gt_matches_multiple() {
        assert!(pattern("orders.>").matches(&subject("orders.created.high_priority")));
    }

    #[test]
    fn gt_no_match_base() {
        assert!(!pattern("orders.>").matches(&subject("orders")));
    }

    #[test]
    fn star_at_start() {
        let p = pattern("*.created");
        assert!(p.matches(&subject("orders.created")));
        assert!(p.matches(&subject("users.created")));
    }

    #[test]
    fn star_in_middle() {
        let p = pattern("metrics.*.host01");
        assert!(p.matches(&subject("metrics.cpu.host01")));
        assert!(!p.matches(&subject("metrics.cpu")));
    }
}
