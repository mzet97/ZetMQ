use dashmap::DashMap;
use parking_lot::RwLock;

use crate::id::SubscriptionId;
use crate::routing::trie::SubjectTrie;
use crate::subject::Subject;
use crate::subject_pattern::{PatternToken, SubjectPattern};

#[derive(Debug)]
pub struct RoutingEngine {
    exact: DashMap<String, Vec<SubscriptionId>>,
    wildcard_trie: RwLock<SubjectTrie>,
}

impl RoutingEngine {
    pub fn new() -> Self {
        Self {
            exact: DashMap::new(),
            wildcard_trie: RwLock::new(SubjectTrie::new()),
        }
    }

    pub fn insert(&self, pattern: &SubjectPattern, sub_id: SubscriptionId) {
        let has_wildcard = pattern
            .tokens()
            .iter()
            .any(|t| matches!(t, PatternToken::SingleWildcard | PatternToken::MultiWildcard));

        if !has_wildcard {
            self.exact
                .entry(pattern.as_str().to_string())
                .or_default()
                .push(sub_id);
        } else {
            let tokens: Vec<String> = pattern
                .tokens()
                .iter()
                .map(|t| match t {
                    PatternToken::Literal(s) => s.clone(),
                    PatternToken::SingleWildcard => "*".to_string(),
                    PatternToken::MultiWildcard => ">".to_string(),
                })
                .collect();
            let has_multi = pattern
                .tokens()
                .iter()
                .any(|t| matches!(t, PatternToken::MultiWildcard));
            self.wildcard_trie.write().insert(&tokens, sub_id, has_multi);
        }
    }

    pub fn remove(&self, pattern: &SubjectPattern, sub_id: SubscriptionId) {
        let has_wildcard = pattern
            .tokens()
            .iter()
            .any(|t| matches!(t, PatternToken::SingleWildcard | PatternToken::MultiWildcard));

        if !has_wildcard {
            if let Some(mut entry) = self.exact.get_mut(pattern.as_str()) {
                entry.retain(|id| *id != sub_id);
            }
        } else {
            let tokens: Vec<String> = pattern
                .tokens()
                .iter()
                .map(|t| match t {
                    PatternToken::Literal(s) => s.clone(),
                    PatternToken::SingleWildcard => "*".to_string(),
                    PatternToken::MultiWildcard => ">".to_string(),
                })
                .collect();
            let has_multi = pattern
                .tokens()
                .iter()
                .any(|t| matches!(t, PatternToken::MultiWildcard));
            self.wildcard_trie.write().remove(&tokens, sub_id, has_multi);
        }
    }

    pub fn match_subject(&self, subject: &Subject) -> Vec<SubscriptionId> {
        let mut results = Vec::new();

        if let Some(subs) = self.exact.get(subject.as_str()) {
            results.extend_from_slice(&*subs);
        }

        let wildcard_subs = self.wildcard_trie.read().match_subject(subject);
        results.extend(wildcard_subs);

        results.sort();
        results.dedup();
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(s: &str) -> Subject {
        Subject::parse(s).unwrap()
    }
    fn pattern(s: &str) -> SubjectPattern {
        SubjectPattern::parse(s).unwrap()
    }

    #[test]
    fn exact_match_routing() {
        let engine = RoutingEngine::new();
        let sub = SubscriptionId::new(1);
        engine.insert(&pattern("orders.created"), sub);
        assert_eq!(
            engine.match_subject(&subject("orders.created")),
            vec![sub]
        );
    }

    #[test]
    fn no_match_different() {
        let engine = RoutingEngine::new();
        engine.insert(&pattern("orders.created"), SubscriptionId::new(1));
        assert!(engine
            .match_subject(&subject("orders.cancelled"))
            .is_empty());
    }

    #[test]
    fn wildcard_star() {
        let engine = RoutingEngine::new();
        let sub = SubscriptionId::new(1);
        engine.insert(&pattern("orders.*"), sub);
        assert_eq!(
            engine.match_subject(&subject("orders.created")),
            vec![sub]
        );
        assert!(engine
            .match_subject(&subject("orders.created.high"))
            .is_empty());
    }

    #[test]
    fn wildcard_gt() {
        let engine = RoutingEngine::new();
        let sub = SubscriptionId::new(1);
        engine.insert(&pattern("orders.>"), sub);
        assert_eq!(
            engine.match_subject(&subject("orders.created")),
            vec![sub]
        );
        assert_eq!(
            engine.match_subject(&subject("orders.created.high")),
            vec![sub]
        );
    }

    #[test]
    fn exact_plus_wildcard_no_duplicates() {
        let engine = RoutingEngine::new();
        let sub1 = SubscriptionId::new(1);
        let sub2 = SubscriptionId::new(2);
        engine.insert(&pattern("orders.created"), sub1);
        engine.insert(&pattern("orders.*"), sub2);

        let matches = engine.match_subject(&subject("orders.created"));
        assert_eq!(matches.len(), 2);
        assert!(matches.contains(&sub1));
        assert!(matches.contains(&sub2));
    }

    #[test]
    fn remove_subscription() {
        let engine = RoutingEngine::new();
        let sub = SubscriptionId::new(1);
        engine.insert(&pattern("test"), sub);
        engine.remove(&pattern("test"), sub);
        assert!(engine.match_subject(&subject("test")).is_empty());
    }
}
