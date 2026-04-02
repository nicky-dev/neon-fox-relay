//! Background worker – consumes the Redis Stream write queue and persists
//! events to PostgreSQL.
//!
//! Architecture:
//! - Reads batches of entries from a Redis Stream consumer group
//! - Deserialises each entry as a [`NostrEvent`]
//! - Calls the appropriate `PgStore` method (insert / upsert)
//! - ACKs the entry on success; leaves it for redelivery on failure
//!
//! The worker runs as a long-lived Tokio task spawned from `main`.  It can
//! also be run as a separate binary by moving `run_worker` into a dedicated
//! `src/bin/worker.rs`.

use crate::{
    config::Config,
    event::NostrEvent,
    storage::{postgres::PgStore, redis::RedisStore},
};
use anyhow::Result;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

const BATCH_SIZE: usize = 50;
const BLOCK_MS: usize = 2_000; // block up to 2 s waiting for new messages
const RETRY_DELAY_SECS: u64 = 5;

/// Spawn the worker task.  Returns a [`tokio::task::JoinHandle`] so `main`
/// can await it for graceful shutdown.
pub fn spawn_worker(
    config: Arc<Config>,
    pg: PgStore,
    redis: RedisStore,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_worker(config, pg, redis).await {
            tracing::error!("Worker terminated with error: {e}");
        }
    })
}

/// Core worker loop.  Runs indefinitely; designed to be robust against
/// transient Redis / PostgreSQL failures.
pub async fn run_worker(
    config: Arc<Config>,
    pg: PgStore,
    mut redis: RedisStore,
) -> Result<()> {
    tracing::info!(
        stream = %config.write_stream_key,
        group  = %config.worker_group,
        "Worker started"
    );

    // Ensure the consumer group exists (idempotent)
    if let Err(e) = redis
        .ensure_consumer_group(&config.write_stream_key, &config.worker_group)
        .await
    {
        tracing::warn!("ensure_consumer_group: {e}");
    }

    loop {
        // Read a batch of pending stream entries
        let entries = match redis
            .read_stream(
                &config.write_stream_key,
                &config.worker_group,
                &config.worker_consumer,
                BATCH_SIZE,
                BLOCK_MS,
            )
            .await
        {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("Redis read_stream failed: {e}; retrying in {RETRY_DELAY_SECS}s");
                sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                continue;
            }
        };

        if entries.is_empty() {
            // Nothing to process – the BLOCK timeout elapsed; loop again
            continue;
        }

        for (entry_id, json) in entries {
            match process_entry(&pg, &entry_id, &json).await {
                Ok(()) => {
                    // ACK on success so Redis removes it from the PEL
                    if let Err(e) = redis
                        .ack_stream(
                            &config.write_stream_key,
                            &config.worker_group,
                            &entry_id,
                        )
                        .await
                    {
                        tracing::warn!("XACK failed for {entry_id}: {e}");
                    }
                }
                Err(e) => {
                    // Leave the entry unACKed so it is redelivered after the
                    // visibility timeout expires.
                    tracing::error!("failed to process stream entry {entry_id}: {e}");
                }
            }
        }
    }
}

/// Process a single stream entry: deserialise and persist to PostgreSQL.
async fn process_entry(pg: &PgStore, entry_id: &str, json: &str) -> Result<()> {
    let event: NostrEvent =
        serde_json::from_str(json).map_err(|e| anyhow::anyhow!("JSON parse: {e}"))?;

    match pg.store_event(&event).await {
        Ok(stored) => {
            if stored {
                tracing::debug!(event_id = %event.id, "Persisted event from stream");
            } else {
                tracing::debug!(event_id = %event.id, entry_id, "Duplicate/stale event; skipped");
            }
            Ok(())
        }
        Err(e) => Err(e),
    }
}
