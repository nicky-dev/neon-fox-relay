//! Subscription module – in-memory subscription manager.
//!
//! Each connected client can open multiple named subscriptions, each with one
//! or more filters.  The manager stores all active subscriptions and provides
//! fast matching: given an event, return the IDs of all clients whose
//! subscriptions match it.
//!
//! The data structures are optimised for the *hot path* (matching every
//! incoming event against all active subscriptions) without any database I/O.

use crate::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// ── Filter ─────────────────────────────────────────────────────────────────────

/// A NIP-01 subscription filter.
///
/// All fields are optional; an event must match *all* provided constraints.
/// Within a field that is a list (e.g. `ids`, `authors`) the event must match
/// *at least one* element (OR semantics).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Filter {
    /// Match events whose ID starts with one of these prefixes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<String>>,

    /// Match events whose pubkey starts with one of these prefixes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authors: Option<Vec<String>>,

    /// Match events of these kinds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<u32>>,

    /// Match events created at or after this Unix timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,

    /// Match events created at or before this Unix timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<i64>,

    /// Maximum number of events to return (used for initial query, not live matching)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    /// Generic tag filters, keyed by single-letter tag name without the `#`
    /// prefix, e.g. `{"e": ["<event_id>"], "p": ["<pubkey>"]}`.
    #[serde(flatten)]
    pub tags: HashMap<String, Vec<String>>,
}

impl Filter {
    /// Returns `true` if `event` satisfies every constraint in this filter.
    pub fn matches(&self, event: &NostrEvent) -> bool {
        // ids – prefix match
        if let Some(ids) = &self.ids {
            if !ids.iter().any(|prefix| event.id.starts_with(prefix.as_str())) {
                return false;
            }
        }

        // authors – prefix match
        if let Some(authors) = &self.authors {
            if !authors
                .iter()
                .any(|prefix| event.pubkey.starts_with(prefix.as_str()))
            {
                return false;
            }
        }

        // kinds
        if let Some(kinds) = &self.kinds {
            if !kinds.contains(&event.kind) {
                return false;
            }
        }

        // since
        if let Some(since) = self.since {
            if event.created_at < since {
                return false;
            }
        }

        // until
        if let Some(until) = self.until {
            if event.created_at > until {
                return false;
            }
        }

        // generic tag filters (e.g. "#e", "#p")
        for (raw_key, expected_values) in &self.tags {
            // The JSON key may be "#e" or "e" depending on serialisation.
            // We normalise by stripping a leading '#'.
            let tag_name = raw_key.trim_start_matches('#');
            if tag_name.len() != 1 {
                // Not a single-letter tag filter key – skip
                continue;
            }

            // Collect all values of this tag in the event
            let event_tag_values: Vec<&str> = event
                .tags
                .iter()
                .filter(|t| t.first().map(|s| s.as_str()) == Some(tag_name))
                .filter_map(|t| t.get(1).map(|s| s.as_str()))
                .collect();

            // At least one expected value must appear in the event's tag list
            if !expected_values
                .iter()
                .any(|v| event_tag_values.contains(&v.as_str()))
            {
                return false;
            }
        }

        true
    }
}

// ── Subscription ───────────────────────────────────────────────────────────────

/// A named subscription held by one client.
#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: String,
    pub filters: Vec<Filter>,
}

impl Subscription {
    pub fn new(id: impl Into<String>, filters: Vec<Filter>) -> Self {
        Self {
            id: id.into(),
            filters,
        }
    }

    /// Returns `true` if the event matches *any* filter in this subscription.
    pub fn matches(&self, event: &NostrEvent) -> bool {
        self.filters.iter().any(|f| f.matches(event))
    }
}

// ── Subscription manager ───────────────────────────────────────────────────────

/// Shared, thread-safe manager for all client subscriptions.
///
/// Layout: `client_id → subscription_id → Subscription`
///
/// The manager is wrapped in an `Arc` so it can be cloned cheaply across
/// Tokio tasks.
#[derive(Debug, Default, Clone)]
pub struct SubscriptionManager {
    /// client_id → map of subscription_id → Subscription
    subs: Arc<RwLock<HashMap<String, HashMap<String, Subscription>>>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a subscription for a client.
    pub async fn add(
        &self,
        client_id: &str,
        subscription_id: impl Into<String>,
        filters: Vec<Filter>,
    ) {
        let sub_id = subscription_id.into();
        let sub = Subscription::new(sub_id.clone(), filters);
        let mut map = self.subs.write().await;
        map.entry(client_id.to_string())
            .or_default()
            .insert(sub_id, sub);
    }

    /// Remove a single subscription for a client.
    pub async fn remove(&self, client_id: &str, subscription_id: &str) {
        let mut map = self.subs.write().await;
        if let Some(client_subs) = map.get_mut(client_id) {
            client_subs.remove(subscription_id);
        }
    }

    /// Remove all subscriptions for a client (called on disconnect).
    pub async fn remove_client(&self, client_id: &str) {
        let mut map = self.subs.write().await;
        map.remove(client_id);
    }

    /// Return the list of `(client_id, subscription_id)` pairs whose filters
    /// match `event`.  Used to fan-out a live event to interested clients.
    pub async fn matching_clients(&self, event: &NostrEvent) -> Vec<(String, String)> {
        let map = self.subs.read().await;
        let mut results = Vec::new();
        for (client_id, subs) in map.iter() {
            for (sub_id, sub) in subs.iter() {
                if sub.matches(event) {
                    results.push((client_id.clone(), sub_id.clone()));
                }
            }
        }
        results
    }

    /// Return the subscriptions held by a specific client (for EOSE delivery).
    pub async fn get_client_subscriptions(&self, client_id: &str) -> Vec<Subscription> {
        let map = self.subs.read().await;
        map.get(client_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Total number of active subscriptions across all clients.
    pub async fn total_subscription_count(&self) -> usize {
        let map = self.subs.read().await;
        map.values().map(|m| m.len()).sum()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(id: &str, pubkey: &str, kind: u32, created_at: i64) -> NostrEvent {
        NostrEvent {
            id: id.to_string(),
            pubkey: pubkey.to_string(),
            created_at,
            kind,
            tags: vec![],
            content: String::new(),
            sig: "s".repeat(128),
        }
    }

    #[test]
    fn filter_kind_match() {
        let f = Filter {
            kinds: Some(vec![1]),
            ..Default::default()
        };
        let e = make_event("id1", "pk1", 1, 1000);
        assert!(f.matches(&e));
    }

    #[test]
    fn filter_kind_no_match() {
        let f = Filter {
            kinds: Some(vec![2]),
            ..Default::default()
        };
        let e = make_event("id1", "pk1", 1, 1000);
        assert!(!f.matches(&e));
    }

    #[test]
    fn filter_since_match() {
        let f = Filter {
            since: Some(500),
            ..Default::default()
        };
        let e = make_event("id1", "pk1", 1, 1000);
        assert!(f.matches(&e));
    }

    #[test]
    fn filter_since_no_match() {
        let f = Filter {
            since: Some(2000),
            ..Default::default()
        };
        let e = make_event("id1", "pk1", 1, 1000);
        assert!(!f.matches(&e));
    }

    #[test]
    fn filter_author_prefix() {
        let f = Filter {
            authors: Some(vec!["abcd".to_string()]),
            ..Default::default()
        };
        let e = make_event("id1", "abcd1234", 1, 1000);
        assert!(f.matches(&e));
    }

    #[test]
    fn filter_tag_match() {
        let mut tags = HashMap::new();
        tags.insert("#e".to_string(), vec!["ref123".to_string()]);
        let f = Filter {
            tags,
            ..Default::default()
        };
        let mut e = make_event("id1", "pk1", 1, 1000);
        e.tags = vec![vec!["e".to_string(), "ref123".to_string()]];
        assert!(f.matches(&e));
    }

    #[tokio::test]
    async fn subscription_manager_add_remove() {
        let mgr = SubscriptionManager::new();
        mgr.add("client1", "sub1", vec![Filter::default()]).await;
        mgr.add("client1", "sub2", vec![Filter::default()]).await;

        let event = make_event("id1", "pk1", 1, 1000);
        let matches = mgr.matching_clients(&event).await;
        assert_eq!(matches.len(), 2);

        mgr.remove("client1", "sub1").await;
        let matches = mgr.matching_clients(&event).await;
        assert_eq!(matches.len(), 1);

        mgr.remove_client("client1").await;
        let matches = mgr.matching_clients(&event).await;
        assert!(matches.is_empty());
    }
}
