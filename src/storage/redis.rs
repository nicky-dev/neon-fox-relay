//! Redis storage / messaging layer.
//!
//! Responsibilities:
//! - **Pub/Sub** – broadcast ephemeral events to all relay instances
//! - **Streams** – enqueue persistent events for the background worker
//! - **Cache**   – hot-path read cache for recently-seen events

use crate::event::NostrEvent;
use anyhow::{Context, Result};
use redis::{
    aio::ConnectionManager,
    streams::{StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands, Client,
};

// ── Connection ─────────────────────────────────────────────────────────────────

/// Create a `ConnectionManager` that automatically reconnects on failure.
pub async fn create_connection_manager(redis_url: &str) -> Result<ConnectionManager> {
    let client = Client::open(redis_url).context("invalid Redis URL")?;
    let manager = ConnectionManager::new(client)
        .await
        .context("failed to connect to Redis")?;
    tracing::info!("Redis connection manager created");
    Ok(manager)
}

// ── Store ──────────────────────────────────────────────────────────────────────

const CACHE_TTL_SECS: u64 = 300; // 5 minutes

/// Thin wrapper around a `ConnectionManager` for Nostr-specific Redis ops.
#[derive(Clone)]
pub struct RedisStore {
    conn: ConnectionManager,
}

impl RedisStore {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }

    // ── Pub/Sub ────────────────────────────────────────────────────────────────

    /// Publish an ephemeral event to the `nostr:ephemeral` channel.
    ///
    /// All relay instances subscribed to this channel will receive it and
    /// fan-out to their local WebSocket clients.
    pub async fn publish_ephemeral(&mut self, event: &NostrEvent) -> Result<()> {
        let payload = serde_json::to_string(event).context("serialize ephemeral event")?;
        self.conn
            .publish::<_, _, ()>("nostr:ephemeral", payload)
            .await
            .context("Redis PUBLISH failed")?;
        Ok(())
    }

    // ── Streams (write queue) ──────────────────────────────────────────────────

    /// Push an event onto the Redis Stream write queue.
    ///
    /// The worker process reads from this stream and persists to PostgreSQL.
    pub async fn enqueue_event(
        &mut self,
        stream_key: &str,
        event: &NostrEvent,
    ) -> Result<String> {
        let payload = serde_json::to_string(event).context("serialize event for stream")?;

        // XADD <stream_key> * event <json>
        let id: String = self
            .conn
            .xadd(stream_key, "*", &[("event", payload)])
            .await
            .context("Redis XADD failed")?;

        Ok(id)
    }

    /// Ensure the consumer group exists for the write queue stream.
    ///
    /// Equivalent to: `XGROUP CREATE <key> <group> $ MKSTREAM`
    pub async fn ensure_consumer_group(
        &mut self,
        stream_key: &str,
        group: &str,
    ) -> Result<()> {
        // XGROUP CREATE … $ MKSTREAM – start reading only new entries;
        // ignore the "group already exists" error.
        let result: redis::RedisResult<()> = self
            .conn
            .xgroup_create_mkstream(stream_key, group, "$")
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("BUSYGROUP") => Ok(()), // already exists
            Err(e) => Err(e).context("XGROUP CREATE failed"),
        }
    }

    /// Read a batch of pending messages from the consumer group.
    ///
    /// Returns a list of `(stream_entry_id, event_json)` pairs.
    pub async fn read_stream(
        &mut self,
        stream_key: &str,
        group: &str,
        consumer: &str,
        count: usize,
        block_ms: usize,
    ) -> Result<Vec<(String, String)>> {
        let opts = StreamReadOptions::default()
            .group(group, consumer)
            .count(count)
            .block(block_ms);

        let reply: StreamReadReply = self
            .conn
            .xread_options(&[stream_key], &[">"], &opts)
            .await
            .context("XREADGROUP failed")?;

        let mut results = Vec::new();
        for stream_key_entry in reply.keys {
            for entry in stream_key_entry.ids {
                let StreamId { id, map } = entry;
                // In redis 0.24 bulk strings are `Value::Data`
                if let Some(redis::Value::Data(bytes)) = map.get("event") {
                    let json = String::from_utf8_lossy(bytes).into_owned();
                    results.push((id, json));
                }
            }
        }
        Ok(results)
    }

    /// Acknowledge a processed stream entry.
    pub async fn ack_stream(
        &mut self,
        stream_key: &str,
        group: &str,
        entry_id: &str,
    ) -> Result<()> {
        self.conn
            .xack::<_, _, _, ()>(stream_key, group, &[entry_id])
            .await
            .context("XACK failed")?;
        Ok(())
    }

    // ── Cache ──────────────────────────────────────────────────────────────────

    /// Cache an event by its ID for `CACHE_TTL_SECS` seconds.
    pub async fn cache_event(&mut self, event: &NostrEvent) -> Result<()> {
        let key = format!("nostr:event:{}", event.id);
        let value = serde_json::to_string(event).context("serialize event for cache")?;
        self.conn
            .set_ex::<_, _, ()>(&key, value, CACHE_TTL_SECS)
            .await
            .context("Redis SET EX failed")?;
        Ok(())
    }

    /// Retrieve a cached event by ID, or `None` if it has expired / never set.
    pub async fn get_cached_event(&mut self, event_id: &str) -> Result<Option<NostrEvent>> {
        let key = format!("nostr:event:{event_id}");
        let value: Option<String> = self
            .conn
            .get(&key)
            .await
            .context("Redis GET failed")?;

        match value {
            None => Ok(None),
            Some(json) => {
                let event =
                    serde_json::from_str(&json).context("deserialize cached event")?;
                Ok(Some(event))
            }
        }
    }
}
