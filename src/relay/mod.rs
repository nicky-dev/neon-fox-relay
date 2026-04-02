//! Relay module – shared application state and event-routing helpers.
//!
//! `AppState` is constructed once at startup and cloned cheaply into every
//! Axum handler via `Arc`.  It bundles all the shared resources (database
//! pool, Redis connection, subscription manager, client registry) in one
//! place so handlers don't need to take many separate `State` extractors.

use crate::{
    config::Config,
    event::NostrEvent,
    storage::{postgres::PgStore, redis::RedisStore},
    subscription::SubscriptionManager,
};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc::UnboundedSender, RwLock};

// ── Client registry ────────────────────────────────────────────────────────────

/// Per-client outbound channel.
///
/// Each WebSocket handler task owns the receiver end; the relay fanout logic
/// sends serialised Nostr messages to the sender end.
pub type ClientTx = UnboundedSender<String>;

/// Thread-safe map of `client_id → outbound channel`.
pub type ClientRegistry = Arc<RwLock<HashMap<String, ClientTx>>>;

// ── Application state ──────────────────────────────────────────────────────────

/// Central application state – shared across all request handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub pg: PgStore,
    pub redis: RedisStore,
    pub subscriptions: SubscriptionManager,
    pub clients: ClientRegistry,
}

impl AppState {
    pub fn new(
        config: Arc<Config>,
        pg: PgStore,
        redis: RedisStore,
    ) -> Self {
        Self {
            config,
            pg,
            redis,
            subscriptions: SubscriptionManager::new(),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new WebSocket client.
    pub async fn add_client(&self, client_id: &str, tx: ClientTx) {
        let mut map = self.clients.write().await;
        map.insert(client_id.to_string(), tx);
    }

    /// Deregister a client and clean up its subscriptions.
    pub async fn remove_client(&self, client_id: &str) {
        {
            let mut map = self.clients.write().await;
            map.remove(client_id);
        }
        self.subscriptions.remove_client(client_id).await;
    }

    /// Fan-out a live event to all clients whose subscriptions match it.
    ///
    /// This is the hot path – it runs entirely in memory without any I/O.
    pub async fn broadcast_event(&self, event: &NostrEvent) -> Result<()> {
        // Find all (client_id, sub_id) pairs whose filters match
        let targets = self.subscriptions.matching_clients(event).await;

        if targets.is_empty() {
            return Ok(());
        }

        let clients = self.clients.read().await;
        for (client_id, sub_id) in targets {
            let msg = serde_json::to_string(&serde_json::json!(["EVENT", sub_id, event]))
                .unwrap_or_default();

            if let Some(tx) = clients.get(&client_id) {
                // Ignore send errors – the client may have just disconnected
                let _ = tx.send(msg);
            }
        }

        Ok(())
    }
}
