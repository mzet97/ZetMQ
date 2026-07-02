use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;
use smallvec::{smallvec, SmallVec};

use crate::id::SubscriptionId;
use crate::routing::trie::SubjectTrie;
use crate::subject::Subject;
use crate::subject_pattern::{PatternToken, SubjectPattern};

pub type MatchResult = SmallVec<[SubscriptionId; 8]>;

#[derive(Debug)]
pub struct RoutingEngine {
    exact: DashMap<String, Vec<SubscriptionId>>,
    wildcard_trie: RwLock<SubjectTrie>,
    /// Fast-path flag: skip wildcard trie traversal when no wildcard subscriptions exist.
    /// Avoids RwLock acquisition per publish for exact-only workloads.
    has_wildcards: AtomicBool,
}

impl Default for RoutingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingEngine {
    pub fn new() -> Self {
        Self {
            exact: DashMap::new(),
            wildcard_trie: RwLock::new(SubjectTrie::new()),
            has_wildcards: AtomicBool::new(false),
        }
    }

    pub fn insert(&self, pattern: &SubjectPattern, sub_id: SubscriptionId) {
        let has_wildcard = pattern.tokens().iter().any(|t| {
            matches!(
                t,
                PatternToken::SingleWildcard | PatternToken::MultiWildcard
            )
        });

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
            self.wildcard_trie
                .write()
                .insert(&tokens, sub_id, has_multi);
            self.has_wildcards.store(true, Ordering::Release);
        }
    }

    pub fn remove(&self, pattern: &SubjectPattern, sub_id: SubscriptionId) {
        let has_wildcard = pattern.tokens().iter().any(|t| {
            matches!(
                t,
                PatternToken::SingleWildcard | PatternToken::MultiWildcard
            )
        });

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
            self.wildcard_trie
                .write()
                .remove(&tokens, sub_id, has_multi);
            // Reset the fast-path flag if the trie is now empty, so exact-only publishes
            // skip the RwLock acquisition. This is a best-effort optimization: the flag is
            // conservative and never produces incorrect results.
            if self.wildcard_trie.read().is_empty() {
                self.has_wildcards.store(false, Ordering::Release);
            }
        }
    }

    pub fn match_subject(&self, subject: &Subject) -> MatchResult {
        let mut results = smallvec![];

        if let Some(subs) = self.exact.get(subject.as_str()) {
            results.extend_from_slice(&subs);
        }

        // Fast path: skip trie traversal when no wildcard subscriptions exist.
        // Avoids RwLock acquisition — a single relaxed atomic load.
        if self.has_wildcards.load(Ordering::Acquire) {
            let wildcard_subs = self.wildcard_trie.read().match_subject(subject);
            // Exact and wildcard subscriptions are structurally disjoint (different insert paths),
            // so a simple extend is safe without dedup
            results.extend(wildcard_subs);
        }

        results
    }

    pub fn has_wildcards(&self) -> bool {
        self.has_wildcards.load(Ordering::Acquire)
    }

    pub fn exact_is_empty(&self, subject_str: &str) -> bool {
        self.exact
            .get(subject_str)
            .map(|subs| subs.is_empty())
            .unwrap_or(true)
    }

    /// Return the single exact subscriber for a subject, if there is exactly one.
    /// This is cheaper than `match_subject` because it avoids allocating a
    /// `MatchResult` and is used by high-throughput publish paths to decide
    /// whether a single-subscriber fast path applies.
    pub fn exact_single_subscriber(&self, subject: &Subject) -> Option<SubscriptionId> {
        self.exact.get(subject.as_str()).and_then(|subs| {
            if subs.len() == 1 {
                Some(subs[0])
            } else {
                None
            }
        })
    }

    /// Match a subject given as a raw string. Avoids constructing a `Subject` when
    /// there are only exact subscriptions and no wildcard subscribers.
    /// Returns the matching subscription IDs and a flag indicating whether a
    /// full `Subject` parse is required for wildcard matching.
    pub fn match_subject_str(&self, subject_str: &str) -> (MatchResult, bool) {
        let mut results = smallvec![];

        if let Some(subs) = self.exact.get(subject_str) {
            results.extend_from_slice(&subs);
        }

        let needs_wildcard_parse = self.has_wildcards.load(Ordering::Acquire);
        (results, needs_wildcard_parse)
    }

    /// Match only wildcard subscriptions for a parsed subject. Used by the
    /// string-subject publish fast path after exact matches have been collected.
    pub fn match_wildcards(&self, subject: &Subject) -> MatchResult {
        if self.has_wildcards.load(Ordering::Acquire) {
            self.wildcard_trie.read().match_subject(subject)
        } else {
            smallvec![]
        }
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
        assert_eq!(engine.match_subject(&subject("orders.created"))[0], sub);
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
        assert_eq!(engine.match_subject(&subject("orders.created"))[0], sub);
        assert!(engine
            .match_subject(&subject("orders.created.high"))
            .is_empty());
    }

    #[test]
    fn wildcard_gt() {
        let engine = RoutingEngine::new();
        let sub = SubscriptionId::new(1);
        engine.insert(&pattern("orders.>"), sub);
        assert_eq!(engine.match_subject(&subject("orders.created"))[0], sub);
        assert_eq!(
            engine.match_subject(&subject("orders.created.high"))[0],
            sub
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

    #[test]
    fn no_wildcards_skips_trie() {
        let engine = RoutingEngine::new();
        // Only exact subscriptions — has_wildcards should be false
        engine.insert(&pattern("orders.created"), SubscriptionId::new(1));
        assert!(!engine.has_wildcards.load(Ordering::Acquire));

        // Should still match exact
        let matches = engine.match_subject(&subject("orders.created"));
        assert_eq!(matches.len(), 1);

        // Now add a wildcard — flag should flip
        engine.insert(&pattern("orders.*"), SubscriptionId::new(2));
        assert!(engine.has_wildcards.load(Ordering::Acquire));
    }

    #[test]
    fn has_wildcards_resets_when_trie_empties() {
        let engine = RoutingEngine::new();
        let sub = SubscriptionId::new(1);
        engine.insert(&pattern("orders.*"), sub);
        assert!(engine.has_wildcards.load(Ordering::Acquire));

        engine.remove(&pattern("orders.*"), sub);
        assert!(
            !engine.has_wildcards.load(Ordering::Acquire),
            "flag should reset to false when all wildcard subscriptions are removed"
        );
    }
}
