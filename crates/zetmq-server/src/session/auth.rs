use zetmq_core::subject_pattern::{PatternToken, SubjectPattern};
use zetmq_core::Subject;

use crate::config::PermissionsConfig;

/// Parsed permission patterns for an authenticated connection.
/// Stored after successful CONNECT auth and checked before PUB/SUB dispatch.
pub struct AuthContext {
    pub username: Option<String>,
    pub publish_patterns: Vec<SubjectPattern>,
    pub subscribe_patterns: Vec<SubjectPattern>,
}

impl AuthContext {
    /// Create an AuthContext from config permissions (no auth / superuser mode).
    pub fn unrestricted() -> Self {
        Self {
            username: None,
            publish_patterns: vec![],
            subscribe_patterns: vec![],
        }
    }

    /// Create an AuthContext from a user's permissions config.
    pub fn from_permissions(username: String, perms: &PermissionsConfig) -> Result<Self, String> {
        let publish_patterns = perms
            .publish
            .iter()
            .map(|p| {
                SubjectPattern::parse(p).map_err(|e| format!("invalid publish pattern '{p}': {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let subscribe_patterns = perms
            .subscribe
            .iter()
            .map(|p| {
                SubjectPattern::parse(p)
                    .map_err(|e| format!("invalid subscribe pattern '{p}': {e}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            username: Some(username),
            publish_patterns,
            subscribe_patterns,
        })
    }

    /// Check if a subject is allowed for publishing.
    /// Empty patterns list means unrestricted (no auth mode).
    pub fn can_publish(&self, subject: &Subject) -> bool {
        if self.publish_patterns.is_empty() {
            return true;
        }
        self.publish_patterns.iter().any(|p| p.matches(subject))
    }

    /// Check if a subscription pattern is allowed.
    /// Empty patterns list means unrestricted (no auth mode).
    pub fn can_subscribe(&self, pattern: &SubjectPattern) -> bool {
        if self.subscribe_patterns.is_empty() {
            return true;
        }
        // Check if any allowed pattern covers the requested pattern.
        // A user can subscribe to X if there's a permission that matches
        // any subject that X would match.
        // Simple approach: check literal overlap by treating the sub pattern
        // as a subject prefix check.
        self.subscribe_patterns
            .iter()
            .any(|allowed| is_pattern_covered(pattern, allowed))
    }
}

/// Check if `requested` pattern is covered by `allowed` pattern.
/// A requested pattern is covered if every subject it can match is also
/// matched by the allowed pattern (i.e., allowed is a superset of requested).
///
/// Examples:
/// - `orders.>` covers `orders.*` and `orders.created.high`
/// - `orders.*` covers `orders.created` but NOT `orders.>` or `orders.a.b`
/// - `>` covers everything
fn is_pattern_covered(requested: &SubjectPattern, allowed: &SubjectPattern) -> bool {
    let req_tokens = requested.tokens();
    let all_tokens = allowed.tokens();

    let mut ri = 0;
    let mut ai = 0;

    while ai < all_tokens.len() && ri < req_tokens.len() {
        match (&all_tokens[ai], &req_tokens[ri]) {
            // Allowed multi-wildcard covers all remaining requested tokens
            (PatternToken::MultiWildcard, _) => return true,
            // Allowed single-wildcard covers one requested literal or wildcard token
            (PatternToken::SingleWildcard, PatternToken::Literal(_))
            | (PatternToken::SingleWildcard, PatternToken::SingleWildcard) => {
                ai += 1;
                ri += 1;
            }
            // Allowed single-wildcard does NOT cover a multi-wildcard (broader scope)
            (PatternToken::SingleWildcard, PatternToken::MultiWildcard) => return false,
            // Allowed literal matches the same requested literal
            (PatternToken::Literal(a), PatternToken::Literal(r)) if a == r => {
                ai += 1;
                ri += 1;
            }
            // Any other mismatch (different literals, literal vs wildcard, etc.)
            _ => return false,
        }
    }

    // Both sequences fully consumed means an exact structural match
    ai == all_tokens.len() && ri == req_tokens.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(s: &str) -> SubjectPattern {
        SubjectPattern::parse(s).unwrap()
    }

    fn subject(s: &str) -> Subject {
        Subject::parse(s).unwrap()
    }

    #[test]
    fn unrestricted_allows_everything() {
        let ctx = AuthContext::unrestricted();
        assert!(ctx.can_publish(&subject("any.thing")));
        assert!(ctx.can_subscribe(&pattern("any.>")));
    }

    #[test]
    fn publish_matches_allowed_pattern() {
        let ctx = AuthContext::from_permissions(
            "user".into(),
            &PermissionsConfig {
                publish: vec!["orders.>".into()],
                subscribe: vec![],
            },
        )
        .unwrap();
        assert!(ctx.can_publish(&subject("orders.created")));
        assert!(ctx.can_publish(&subject("orders.updated")));
        assert!(!ctx.can_publish(&subject("events.test")));
    }

    #[test]
    fn subscribe_pattern_covered() {
        let ctx = AuthContext::from_permissions(
            "user".into(),
            &PermissionsConfig {
                publish: vec![],
                subscribe: vec!["orders.>".into()],
            },
        )
        .unwrap();
        assert!(ctx.can_subscribe(&pattern("orders.*")));
        assert!(ctx.can_subscribe(&pattern("orders.created")));
        assert!(!ctx.can_subscribe(&pattern("events.>")));
    }

    #[test]
    fn star_does_not_cover_multi_wildcard_or_multi_token() {
        let ctx = AuthContext::from_permissions(
            "user".into(),
            &PermissionsConfig {
                publish: vec![],
                subscribe: vec!["orders.*".into()],
            },
        )
        .unwrap();
        // orders.* DOES cover single-token literals
        assert!(ctx.can_subscribe(&pattern("orders.created")));
        assert!(ctx.can_subscribe(&pattern("orders.*")));
        // orders.* does NOT cover orders.> (broader multi-level wildcard)
        assert!(!ctx.can_subscribe(&pattern("orders.>")));
        // orders.* does NOT cover multi-token literals
        assert!(!ctx.can_subscribe(&pattern("orders.created.high")));
    }

    #[test]
    fn full_wildcard_covers_all() {
        let ctx = AuthContext::from_permissions(
            "admin".into(),
            &PermissionsConfig {
                publish: vec![">".into()],
                subscribe: vec![">".into()],
            },
        )
        .unwrap();
        assert!(ctx.can_publish(&subject("anything")));
        assert!(ctx.can_subscribe(&pattern("everything.>")));
    }
}
