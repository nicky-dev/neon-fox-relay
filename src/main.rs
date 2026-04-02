//! neon-fox-relay – main entry point.
//!
//! Initialises all shared resources and starts:
//! - The Axum HTTP/WebSocket server
//! - The background worker task (Redis Stream → PostgreSQL)
//! - Graceful shutdown on SIGTERM / Ctrl-C

mod config;
mod event;
mod relay;
mod service;
mod storage;
mod subscription;
mod utils;
mod websocket;
mod worker;

use crate::{
    config::Config,
    relay::AppState,
    storage::{postgres, redis as redis_storage},
    websocket::ws_handler,
    worker::spawn_worker,
};
use anyhow::Result;
use axum::{
    routing::get,
    Router,
};
use std::{net::SocketAddr, sync::Arc};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // ── Logging ────────────────────────────────────────────────────────────────
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // ── Configuration ──────────────────────────────────────────────────────────
    let config = Arc::new(Config::from_env()?);
    tracing::info!(
        name    = %config.relay_name,
        address = %config.bind_addr(),
        "Starting neon-fox-relay"
    );

    // ── Database connections ───────────────────────────────────────────────────
    let pg_pool = postgres::create_pool(&config.database_url, config.database_max_connections)
        .await?;
    let pg_store = storage::postgres::PgStore::new(pg_pool);

    let redis_conn =
        redis_storage::create_connection_manager(&config.redis_url).await?;
    let redis_store = redis_storage::RedisStore::new(redis_conn);

    // ── Application state ──────────────────────────────────────────────────────
    let state = Arc::new(AppState::new(
        Arc::clone(&config),
        pg_store.clone(),
        redis_store.clone(),
    ));

    // ── Background worker ──────────────────────────────────────────────────────
    let _worker_handle = spawn_worker(
        Arc::clone(&config),
        pg_store,
        redis_store,
    );

    // ── Axum router ────────────────────────────────────────────────────────────
    let app = Router::new()
        // WebSocket endpoint – clients connect here
        .route("/", get(ws_handler))
        // Health check
        .route("/health", get(health_handler))
        // Relay information document (NIP-11)
        .route("/relay-info", get({
            let cfg = Arc::clone(&config);
            move || relay_info_handler(cfg)
        }))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // ── Bind & serve ──────────────────────────────────────────────────────────
    let addr: SocketAddr = config.bind_addr().parse()?;
    tracing::info!(%addr, "Listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("Server shut down gracefully");
    Ok(())
}

// ── Health check ─────────────────────────────────────────────────────────────

async fn health_handler() -> &'static str {
    "OK"
}

// ── NIP-11 relay information document ────────────────────────────────────────

/// Returns a minimal NIP-11 relay information JSON document.
/// Clients request this with `Accept: application/nostr+json`.
async fn relay_info_handler(config: Arc<Config>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "name":        config.relay_name,
        "description": config.relay_description,
        "pubkey":      config.relay_pubkey,
        "contact":     config.relay_contact,
        "supported_nips": [1, 11, 16, 33],
        "software":    "https://github.com/nicky-dev/neon-fox-relay",
        "version":     env!("CARGO_PKG_VERSION"),
        "limitation": {
            "max_message_length": 524288,
            "max_subscriptions": 20,
            "max_filters": 10,
            "max_tags": 2000,
        }
    }))
}

// ── Graceful shutdown ─────────────────────────────────────────────────────────

/// Returns a future that completes when the process receives SIGTERM or Ctrl-C.
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c   => {},
        _ = terminate => {},
    }

    tracing::info!("Shutdown signal received");
}
