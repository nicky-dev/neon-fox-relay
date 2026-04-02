//! WebSocket handler module.
//!
//! Each incoming WebSocket connection is handled by [`handle_socket`].
//! The handler:
//!
//! 1. Assigns a unique client ID
//! 2. Registers an outbound channel in the client registry
//! 3. Spawns an outbound-message forwarding task
//! 4. Loops on incoming frames, parsing and dispatching Nostr messages
//! 5. Cleans up on disconnect

use crate::{
    event::NostrEvent,
    relay::AppState,
    service::handle_event,
    subscription::Filter,
    utils::new_client_id,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc;

// ── Axum upgrade handler ───────────────────────────────────────────────────────

/// Axum route handler: upgrade an HTTP request to a WebSocket connection.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

// ── Per-connection handler ─────────────────────────────────────────────────────

/// Drive a single WebSocket connection to completion.
async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let client_id = new_client_id();
    tracing::debug!(client_id, "Client connected");

    // Unbounded channel: relay fanout → this client
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Register the client
    state.add_client(&client_id, tx).await;

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // ── Outbound forwarding task ───────────────────────────────────────────────
    // Reads from the in-memory channel and writes to the WebSocket.
    let sender_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
        // Attempt a clean close frame
        let _ = ws_sender.close().await;
    });

    // ── Inbound message loop ───────────────────────────────────────────────────
    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                if let Err(e) = dispatch_message(&state, &client_id, &text).await {
                    tracing::warn!(client_id, "message dispatch error: {e}");
                    // Send a NOTICE with the error back to the client
                    let notice = notice_msg(&format!("error: {e}"));
                    let clients = state.clients.read().await;
                    if let Some(tx) = clients.get(&client_id) {
                        let _ = tx.send(notice);
                    }
                }
            }
            Message::Ping(data) => {
                // axum's WebSocket layer sends automatic Pong; nothing to do
                tracing::trace!(client_id, bytes = data.len(), "Ping received");
            }
            Message::Close(_) => {
                tracing::debug!(client_id, "Close frame received");
                break;
            }
            _ => {}
        }
    }

    // ── Cleanup ────────────────────────────────────────────────────────────────
    state.remove_client(&client_id).await;
    sender_task.abort();
    tracing::debug!(client_id, "Client disconnected");
}

// ── Message dispatcher ─────────────────────────────────────────────────────────

/// Parse a raw text frame and dispatch to the appropriate handler.
///
/// Nostr messages are JSON arrays: `["TYPE", ...]`
async fn dispatch_message(
    state: &Arc<AppState>,
    client_id: &str,
    text: &str,
) -> anyhow::Result<()> {
    let value: Value =
        serde_json::from_str(text).map_err(|e| anyhow::anyhow!("JSON parse: {e}"))?;

    let arr = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("message is not a JSON array"))?;

    let msg_type = arr
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing message type"))?;

    match msg_type {
        "EVENT" => handle_client_event(state, client_id, arr).await,
        "REQ" => handle_client_req(state, client_id, arr).await,
        "CLOSE" => handle_client_close(state, client_id, arr).await,
        other => Err(anyhow::anyhow!("unknown message type: {other}")),
    }
}

// ── ["EVENT", <event>] ────────────────────────────────────────────────────────

async fn handle_client_event(
    state: &Arc<AppState>,
    client_id: &str,
    arr: &[Value],
) -> anyhow::Result<()> {
    let event_val = arr
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("EVENT message missing event object"))?;

    let event: NostrEvent = serde_json::from_value(event_val.clone())
        .map_err(|e| anyhow::anyhow!("invalid event structure: {e}"))?;

    let event_id = event.id.clone();
    let result = handle_event(state, event).await;

    // Send ["OK", <id>, <accepted>, <message>] back to the sender
    let ok_msg = serde_json::to_string(&serde_json::json!([
        "OK",
        event_id,
        result.accepted,
        result.message,
    ]))?;

    let clients = state.clients.read().await;
    if let Some(tx) = clients.get(client_id) {
        let _ = tx.send(ok_msg);
    }

    Ok(())
}

// ── ["REQ", <sub_id>, <filter>...] ───────────────────────────────────────────

async fn handle_client_req(
    state: &Arc<AppState>,
    client_id: &str,
    arr: &[Value],
) -> anyhow::Result<()> {
    let sub_id = arr
        .get(1)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("REQ message missing subscription ID"))?
        .to_string();

    // Parse all filter objects (arr[2..])
    let filters: Vec<Filter> = arr[2..]
        .iter()
        .map(|v| {
            serde_json::from_value::<Filter>(v.clone())
                .map_err(|e| anyhow::anyhow!("invalid filter: {e}"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Enforce filter count limit
    if filters.len() > state.config.max_filters_per_subscription {
        return Err(anyhow::anyhow!(
            "too many filters (max {})",
            state.config.max_filters_per_subscription
        ));
    }

    // Enforce subscription limit
    let subs = state
        .subscriptions
        .get_client_subscriptions(client_id)
        .await;
    if subs.len() >= state.config.max_subscriptions_per_client {
        return Err(anyhow::anyhow!(
            "too many subscriptions (max {})",
            state.config.max_subscriptions_per_client
        ));
    }

    // Register subscription in memory
    state
        .subscriptions
        .add(client_id, &sub_id, filters.clone())
        .await;

    // Query historical events from PostgreSQL for each filter and send them
    for filter in &filters {
        // Honour the per-filter limit, capped at the relay's configured maximum
        let limit = filter
            .limit
            .unwrap_or(100)
            .min(state.config.max_subscriptions_per_client * 50); // sensible cap
        let authors: Option<Vec<String>> = filter.authors.clone();
        let kinds: Option<Vec<u32>> = filter.kinds.clone();

        let events = state
            .pg
            .query_events(
                authors.as_deref(),
                kinds.as_deref(),
                filter.since,
                filter.until,
                limit,
            )
            .await
            .unwrap_or_default();

        let clients = state.clients.read().await;
        if let Some(tx) = clients.get(client_id) {
            for event in events {
                let msg = serde_json::to_string(
                    &serde_json::json!(["EVENT", sub_id, event]),
                )
                .unwrap_or_default();
                let _ = tx.send(msg);
            }
        }
    }

    // Send EOSE (End Of Stored Events)
    let eose = serde_json::to_string(&serde_json::json!(["EOSE", sub_id]))?;
    let clients = state.clients.read().await;
    if let Some(tx) = clients.get(client_id) {
        let _ = tx.send(eose);
    }

    Ok(())
}

// ── ["CLOSE", <sub_id>] ───────────────────────────────────────────────────────

async fn handle_client_close(
    state: &Arc<AppState>,
    client_id: &str,
    arr: &[Value],
) -> anyhow::Result<()> {
    let sub_id = arr
        .get(1)
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("CLOSE message missing subscription ID"))?;

    state.subscriptions.remove(client_id, sub_id).await;
    tracing::debug!(client_id, sub_id, "Subscription closed");
    Ok(())
}

// ── Helper ────────────────────────────────────────────────────────────────────

/// Build a `["NOTICE", message]` JSON string.
fn notice_msg(message: &str) -> String {
    serde_json::to_string(&serde_json::json!(["NOTICE", message]))
        .unwrap_or_else(|_| r#"["NOTICE","internal error"]"#.to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn notice_msg_format() {
        let msg = notice_msg("test error");
        let parsed: Value = serde_json::from_str(&msg).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr[0], "NOTICE");
        assert_eq!(arr[1], "test error");
    }

    #[test]
    fn notice_msg_empty_string() {
        let msg = notice_msg("");
        let parsed: Value = serde_json::from_str(&msg).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr[0], "NOTICE");
        assert_eq!(arr[1], "");
    }

    // ── dispatch_message parsing helpers ──────────────────────────────────────
    // These tests validate the JSON parsing logic without requiring a live
    // AppState / database connection.

    fn parse_msg_type(text: &str) -> anyhow::Result<String> {
        let value: Value = serde_json::from_str(text)
            .map_err(|e| anyhow::anyhow!("JSON parse: {e}"))?;
        let arr = value
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("message is not a JSON array"))?;
        let msg_type = arr
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("missing message type"))?;
        Ok(msg_type.to_string())
    }

    #[test]
    fn parse_event_message_type() {
        let text = r#"["EVENT", {}]"#;
        assert_eq!(parse_msg_type(text).unwrap(), "EVENT");
    }

    #[test]
    fn parse_req_message_type() {
        let text = r#"["REQ", "sub1", {}]"#;
        assert_eq!(parse_msg_type(text).unwrap(), "REQ");
    }

    #[test]
    fn parse_close_message_type() {
        let text = r#"["CLOSE", "sub1"]"#;
        assert_eq!(parse_msg_type(text).unwrap(), "CLOSE");
    }

    #[test]
    fn parse_invalid_json_fails() {
        let result = parse_msg_type("not json");
        assert!(result.is_err());
    }

    #[test]
    fn parse_non_array_fails() {
        let result = parse_msg_type(r#"{"type": "EVENT"}"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not a JSON array"));
    }

    #[test]
    fn parse_empty_array_fails() {
        let result = parse_msg_type("[]");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing message type"));
    }

    // ── Filter parsing tests ──────────────────────────────────────────────────

    #[test]
    fn parse_req_filters() {
        let text = r#"["REQ", "sub1", {"kinds": [1]}, {"kinds": [0, 3]}]"#;
        let value: Value = serde_json::from_str(text).unwrap();
        let arr = value.as_array().unwrap();

        let filters: Vec<Filter> = arr[2..]
            .iter()
            .map(|v| serde_json::from_value::<Filter>(v.clone()).unwrap())
            .collect();

        assert_eq!(filters.len(), 2);
        assert_eq!(filters[0].kinds.as_ref().unwrap(), &[1u32]);
        assert_eq!(filters[1].kinds.as_ref().unwrap(), &[0u32, 3u32]);
    }

    #[test]
    fn parse_req_no_filters() {
        let text = r#"["REQ", "sub1"]"#;
        let value: Value = serde_json::from_str(text).unwrap();
        let arr = value.as_array().unwrap();

        let filters: Vec<Filter> = arr[2..]
            .iter()
            .map(|v| serde_json::from_value::<Filter>(v.clone()).unwrap())
            .collect();

        assert!(filters.is_empty());
    }
}
