use std::collections::HashMap;

use smallvec::{smallvec, SmallVec};

use crate::id::SubscriptionId;
use crate::subject::Subject;

pub type MatchResult = SmallVec<[SubscriptionId; 8]>;

#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    multi_wildcard_subs: Vec<SubscriptionId>,
    exact_subs: Vec<SubscriptionId>,
}

#[derive(Debug, Default)]
pub struct SubjectTrie {
    root: TrieNode,
    /// Total number of subscribers across all nodes.
    /// Incremented on insert, decremented on remove. Used for O(1) emptiness check.
    subscriber_count: usize,
}

impl SubjectTrie {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true when no wildcard subscriptions remain in the trie.
    pub fn is_empty(&self) -> bool {
        self.subscriber_count == 0
    }

    pub fn insert(&mut self, tokens: &[String], sub_id: SubscriptionId, has_multi_wildcard: bool) {
        let mut node = &mut self.root;

        for (i, token) in tokens.iter().enumerate() {
            if has_multi_wildcard && i == tokens.len() - 1 {
                node.multi_wildcard_subs.push(sub_id);
                self.subscriber_count += 1;
                return;
            }
            node = node.children.entry(token.clone()).or_default();
        }

        node.exact_subs.push(sub_id);
        self.subscriber_count += 1;
    }

    pub fn remove(&mut self, tokens: &[String], sub_id: SubscriptionId, has_multi_wildcard: bool) {
        let mut node = &mut self.root;

        for (i, token) in tokens.iter().enumerate() {
            if has_multi_wildcard && i == tokens.len() - 1 {
                let before = node.multi_wildcard_subs.len();
                node.multi_wildcard_subs.retain(|id| *id != sub_id);
                if node.multi_wildcard_subs.len() < before {
                    self.subscriber_count = self.subscriber_count.saturating_sub(1);
                }
                return;
            }
            match node.children.get_mut(token) {
                Some(child) => node = child,
                None => return,
            }
        }

        let before = node.exact_subs.len();
        node.exact_subs.retain(|id| *id != sub_id);
        if node.exact_subs.len() < before {
            self.subscriber_count = self.subscriber_count.saturating_sub(1);
        }
    }

    pub fn match_subject(&self, subject: &Subject) -> MatchResult {
        let tokens = subject.tokens();
        let mut results = smallvec![];

        Self::match_recursive(&self.root, tokens, 0, &mut results);

        results
    }

    fn match_recursive(
        node: &TrieNode,
        tokens: &[String],
        index: usize,
        results: &mut MatchResult,
    ) {
        // Multi-wildcard at this node matches 1+ remaining tokens
        if !node.multi_wildcard_subs.is_empty() && index < tokens.len() {
            results.extend_from_slice(&node.multi_wildcard_subs);
        }

        if index == tokens.len() {
            results.extend_from_slice(&node.exact_subs);
            return;
        }

        // Exact child match
        if let Some(child) = node.children.get(&tokens[index]) {
            Self::match_recursive(child, tokens, index + 1, results);
        }

        // Single wildcard (*) child: matches exactly one token
        if let Some(star_child) = node.children.get("*") {
            // Avoid double-visiting if the subject token happens to be "*"
            if tokens[index] != "*" {
                Self::match_recursive(star_child, tokens, index + 1, results);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_match_exact() {
        let mut trie = SubjectTrie::new();
        let sub = SubscriptionId::new(1);
        trie.insert(&["orders".into(), "created".into()], sub, false);

        let subj = Subject::parse("orders.created").unwrap();
        let mut expected = MatchResult::new();
        expected.push(sub);
        assert_eq!(trie.match_subject(&subj), expected);
    }

    #[test]
    fn no_match_different_subject() {
        let mut trie = SubjectTrie::new();
        trie.insert(
            &["orders".into(), "created".into()],
            SubscriptionId::new(1),
            false,
        );
        let subj = Subject::parse("orders.cancelled").unwrap();
        assert!(trie.match_subject(&subj).is_empty());
    }

    #[test]
    fn multi_wildcard_match() {
        let mut trie = SubjectTrie::new();
        let sub = SubscriptionId::new(10);
        trie.insert(&["orders".into(), ">".into()], sub, true);

        let s1 = Subject::parse("orders.created").unwrap();
        assert!(trie.match_subject(&s1).contains(&sub));

        let s2 = Subject::parse("orders.created.high").unwrap();
        assert!(trie.match_subject(&s2).contains(&sub));
    }

    #[test]
    fn remove_subscription() {
        let mut trie = SubjectTrie::new();
        let sub = SubscriptionId::new(1);
        trie.insert(&["test".into()], sub, false);
        trie.remove(&["test".into()], sub, false);

        let subj = Subject::parse("test").unwrap();
        assert!(trie.match_subject(&subj).is_empty());
    }
}
