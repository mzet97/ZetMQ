use std::collections::HashMap;

use crate::id::SubscriptionId;
use crate::subject::Subject;

#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    multi_wildcard_subs: Vec<SubscriptionId>,
    exact_subs: Vec<SubscriptionId>,
}

#[derive(Debug, Default)]
pub struct SubjectTrie {
    root: TrieNode,
}

impl SubjectTrie {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, tokens: &[String], sub_id: SubscriptionId, has_multi_wildcard: bool) {
        let mut node = &mut self.root;

        for (i, token) in tokens.iter().enumerate() {
            if has_multi_wildcard && i == tokens.len() - 1 {
                node.multi_wildcard_subs.push(sub_id);
                return;
            }
            node = node.children.entry(token.clone()).or_default();
        }

        node.exact_subs.push(sub_id);
    }

    pub fn remove(&mut self, tokens: &[String], sub_id: SubscriptionId, has_multi_wildcard: bool) {
        let mut node = &mut self.root;

        for (i, token) in tokens.iter().enumerate() {
            if has_multi_wildcard && i == tokens.len() - 1 {
                node.multi_wildcard_subs.retain(|id| *id != sub_id);
                return;
            }
            match node.children.get_mut(token) {
                Some(child) => node = child,
                None => return,
            }
        }

        node.exact_subs.retain(|id| *id != sub_id);
    }

    pub fn match_subject(&self, subject: &Subject) -> Vec<SubscriptionId> {
        let tokens = subject.tokens();
        let mut results = Vec::with_capacity(8);

        Self::match_recursive(&self.root, tokens, 0, &mut results);

        results
    }

    fn match_recursive(
        node: &TrieNode,
        tokens: &[String],
        index: usize,
        results: &mut Vec<SubscriptionId>,
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
        assert_eq!(trie.match_subject(&subj), vec![sub]);
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
