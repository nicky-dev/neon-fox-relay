//! Event service – orchestrates the full NIP-01 EVENT flow.
//!
//! Steps (as specified in the architecture requirements):
//! 1. Parse JSON (done upstream in the WebSocket handler)
//! 2. Validate structure
//! 3. Verify event ID (SHA-256 hash)
//! 4. Verify secp256k1 Schnorr signature
//! 5. Check for duplicates
//! 6. Route:
//!    - ephemeral  → Redis pub/sub only
//!    - replaceable / param-replaceable → upsert
//!    - normal     → insert
//! 7. Push to Redis Stream (write queue)
//! 8. Broadcast to matching local subscribers

use crate::{event::NostrEvent, relay::AppState};

/// Result of processing an incoming EVENT message.
#[derive(Debug)]
pub struct EventResult {
    /// Was the event accepted (stored / forwarded)?
    pub accepted: bool,
    /// Human-readable status message sent back to the client
    pub message: String,
}

/// Process a fully-parsed [`NostrEvent`] through the relay pipeline.
///
/// Returns an [`EventResult`] that is used to build the `["OK", …]` reply.
pub async fn handle_event(state: &AppState, event: NostrEvent) -> EventResult {
    // ── Relay limits ──────────────────────────────────────────────────────────
    if event.content.len() > state.config.max_content_length {
        return EventResult {
            accepted: false,
            message: format!(
                "invalid: content exceeds maximum length of {} bytes",
                state.config.max_content_length
            ),
        };
    }

    if event.tags.len() > state.config.max_event_tags {
        return EventResult {
            accepted: false,
            message: format!(
                "invalid: too many tags (max {})",
                state.config.max_event_tags
            ),
        };
    }

    // ── Step 2 + 3 + 4: validate (structure, ID hash, signature) ──────────────
    if let Err(e) = event.validate() {
        return EventResult {
            accepted: false,
            message: format!("invalid: {e}"),
        };
    }

    let is_ephemeral = event.is_ephemeral();

    // ── Step 5: duplicate check (skip for ephemeral – they're never stored) ───
    if !is_ephemeral {
        match state.pg.event_exists(&event.id).await {
            Ok(true) => {
                return EventResult {
                    accepted: false,
                    message: "duplicate: event already stored".to_string(),
                };
            }
            Err(e) => {
                tracing::error!("DB duplicate check failed: {e}");
                return EventResult {
                    accepted: false,
                    message: "error: internal server error".to_string(),
                };
            }
            Ok(false) => {}
        }
    }

    // ── Step 6: route by category ─────────────────────────────────────────────
    let mut redis = state.redis.clone();

    if is_ephemeral {
        // Ephemeral: only pub/sub, never stored
        if let Err(e) = redis.publish_ephemeral(&event).await {
            tracing::warn!("failed to publish ephemeral event: {e}");
        }
    } else {
        // ── Step 7: push to Redis Stream (async persistence queue) ────────────
        if let Err(e) = redis
            .enqueue_event(&state.config.write_stream_key, &event)
            .await
        {
            tracing::error!("failed to enqueue event in Redis stream: {e}");
            return EventResult {
                accepted: false,
                message: "error: failed to enqueue event".to_string(),
            };
        }
    }

    // ── Step 8: broadcast to matching local subscribers ───────────────────────
    if let Err(e) = state.broadcast_event(&event).await {
        tracing::warn!("broadcast_event error: {e}");
    }

    // Also cache the event for fast reads
    if let Err(e) = redis.cache_event(&event).await {
        tracing::debug!("cache_event failed (non-fatal): {e}");
    }

    EventResult {
        accepted: true,
        message: String::new(),
    }
}
