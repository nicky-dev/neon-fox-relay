//! Configuration module – loads all relay settings from environment variables.
//!
//! Uses `dotenvy` to read a `.env` file (optional) and then reads each
//! variable from the process environment.  All settings have sensible
//! defaults so the relay can start with minimal configuration.

use anyhow::{Context, Result};
use std::env;

/// Top-level relay configuration, loaded once at startup and shared via `Arc`.
#[derive(Debug, Clone)]
pub struct Config {
    // ── Network ──────────────────────────────────────────────────────────────
    /// Bind address, e.g. `"0.0.0.0"`
    pub host: String,
    /// Bind port, e.g. `8080`
    pub port: u16,

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    /// Full connection string, e.g. `"postgres://user:pass@host/db"`
    pub database_url: String,
    /// Maximum connections in the Postgres pool
    pub database_max_connections: u32,

    // ── Redis ─────────────────────────────────────────────────────────────────
    /// Redis connection URL, e.g. `"redis://127.0.0.1:6379"`
    pub redis_url: String,

    // ── Relay metadata ────────────────────────────────────────────────────────
    pub relay_name: String,
    pub relay_description: String,
    pub relay_pubkey: String,
    pub relay_contact: String,

    // ── Redis stream / worker ─────────────────────────────────────────────────
    /// Redis stream key used as the write queue
    pub write_stream_key: String,
    /// Consumer group name for the Redis stream
    pub worker_group: String,
    /// Consumer name for this worker instance
    pub worker_consumer: String,

    // ── Limits ────────────────────────────────────────────────────────────────
    pub max_event_tags: usize,
    pub max_content_length: usize,
    pub max_subscriptions_per_client: usize,
    pub max_filters_per_subscription: usize,
}

impl Config {
    /// Load configuration from the environment (and an optional `.env` file).
    ///
    /// # Errors
    /// Returns an error if a required variable is missing or cannot be parsed.
    pub fn from_env() -> Result<Self> {
        // Load `.env` if present; ignore error if the file doesn't exist
        let _ = dotenvy::dotenv();

        Ok(Config {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: parse_env("PORT", 8080)?,

            database_url: env::var("DATABASE_URL")
                .context("DATABASE_URL must be set")?,
            database_max_connections: parse_env("DATABASE_MAX_CONNECTIONS", 20)?,

            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),

            relay_name: env::var("RELAY_NAME")
                .unwrap_or_else(|_| "neon-fox-relay".to_string()),
            relay_description: env::var("RELAY_DESCRIPTION")
                .unwrap_or_default(),
            relay_pubkey: env::var("RELAY_PUBKEY").unwrap_or_default(),
            relay_contact: env::var("RELAY_CONTACT").unwrap_or_default(),

            write_stream_key: env::var("WRITE_STREAM_KEY")
                .unwrap_or_else(|_| "nostr:events:stream".to_string()),
            worker_group: env::var("WORKER_GROUP")
                .unwrap_or_else(|_| "nostr-workers".to_string()),
            worker_consumer: env::var("WORKER_CONSUMER")
                .unwrap_or_else(|_| "worker-1".to_string()),

            max_event_tags: parse_env("MAX_EVENT_TAGS", 2000)?,
            max_content_length: parse_env("MAX_CONTENT_LENGTH", 65536)?,
            max_subscriptions_per_client: parse_env("MAX_SUBSCRIPTIONS_PER_CLIENT", 20)?,
            max_filters_per_subscription: parse_env("MAX_FILTERS_PER_SUBSCRIPTION", 10)?,
        })
    }

    /// Convenience: `"host:port"` bind address string.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Parse an environment variable into `T`, falling back to `default` when the
/// variable is absent or empty.
fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + std::fmt::Debug,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(val) if !val.is_empty() => val
            .parse::<T>()
            .with_context(|| format!("failed to parse env var {key}={val:?}")),
        _ => Ok(default),
    }
}
