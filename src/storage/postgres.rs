//! PostgreSQL storage layer.
//!
//! Provides async, pooled access to the `events` table via `sqlx`.
//! Query macros are intentionally avoided so that no live database is required
//! at compile time.

use crate::event::{EventCategory, NostrEvent};
use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

// ── Connection pool ────────────────────────────────────────────────────────────

/// Create and return a PostgreSQL connection pool.
pub async fn create_pool(database_url: &str, max_connections: u32) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
        .context("failed to connect to PostgreSQL")?;

    tracing::info!("PostgreSQL pool created (max_connections={max_connections})");
    Ok(pool)
}

// ── Store ──────────────────────────────────────────────────────────────────────

/// Thin wrapper around a `PgPool` for event-related operations.
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    /// Persist a **regular** event (INSERT … ON CONFLICT DO NOTHING).
    ///
    /// Returns `true` if the row was inserted, `false` if a duplicate was found.
    pub async fn insert_event(&self, event: &NostrEvent) -> Result<bool> {
        let tags_json =
            serde_json::to_value(&event.tags).context("failed to serialize tags")?;

        let result = sqlx::query(
            r#"
            INSERT INTO events (id, pubkey, created_at, kind, tags, content, sig)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&event.id)
        .bind(&event.pubkey)
        .bind(event.created_at)
        .bind(event.kind as i32)
        .bind(&tags_json)
        .bind(&event.content)
        .bind(&event.sig)
        .execute(&self.pool)
        .await
        .context("insert_event failed")?;

        Ok(result.rows_affected() > 0)
    }

    /// Upsert a **replaceable** event (NIP-16).
    ///
    /// Keeps only the newest event for (pubkey, kind).  If the stored event is
    /// already newer, the incoming one is silently dropped.
    pub async fn upsert_replaceable_event(&self, event: &NostrEvent) -> Result<bool> {
        let tags_json =
            serde_json::to_value(&event.tags).context("failed to serialize tags")?;

        // Delete the old event first (if it's older) then insert the new one.
        // This is done inside a transaction for consistency.
        let mut tx = self.pool.begin().await.context("begin transaction")?;

        sqlx::query(
            r#"
            DELETE FROM events
            WHERE pubkey = $1 AND kind = $2 AND created_at < $3
            "#,
        )
        .bind(&event.pubkey)
        .bind(event.kind as i32)
        .bind(event.created_at)
        .execute(&mut *tx)
        .await
        .context("delete old replaceable event")?;

        let result = sqlx::query(
            r#"
            INSERT INTO events (id, pubkey, created_at, kind, tags, content, sig)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&event.id)
        .bind(&event.pubkey)
        .bind(event.created_at)
        .bind(event.kind as i32)
        .bind(&tags_json)
        .bind(&event.content)
        .bind(&event.sig)
        .execute(&mut *tx)
        .await
        .context("insert replaceable event")?;

        tx.commit().await.context("commit transaction")?;
        Ok(result.rows_affected() > 0)
    }

    /// Upsert a **parameterized replaceable** event (NIP-33).
    ///
    /// Keeps only the newest event for (pubkey, kind, d-tag).
    pub async fn upsert_param_replaceable_event(&self, event: &NostrEvent) -> Result<bool> {
        let tags_json =
            serde_json::to_value(&event.tags).context("failed to serialize tags")?;
        let d_tag = event.d_tag().to_string();

        let mut tx = self.pool.begin().await.context("begin transaction")?;

        // Delete the older event whose d-tag matches
        sqlx::query(
            r#"
            DELETE FROM events
            WHERE pubkey = $1
              AND kind   = $2
              AND created_at < $3
              AND tags @> $4::jsonb
            "#,
        )
        .bind(&event.pubkey)
        .bind(event.kind as i32)
        .bind(event.created_at)
        .bind(serde_json::json!([["d", d_tag]]))
        .execute(&mut *tx)
        .await
        .context("delete old param-replaceable event")?;

        let result = sqlx::query(
            r#"
            INSERT INTO events (id, pubkey, created_at, kind, tags, content, sig)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&event.id)
        .bind(&event.pubkey)
        .bind(event.created_at)
        .bind(event.kind as i32)
        .bind(&tags_json)
        .bind(&event.content)
        .bind(&event.sig)
        .execute(&mut *tx)
        .await
        .context("insert param-replaceable event")?;

        tx.commit().await.context("commit transaction")?;
        Ok(result.rows_affected() > 0)
    }

    /// High-level dispatch: choose the right write strategy based on event category.
    ///
    /// Returns `true` when the event was persisted (new/updated), `false` for
    /// duplicates or when the stored version is already newer.
    pub async fn store_event(&self, event: &NostrEvent) -> Result<bool> {
        match event.category() {
            EventCategory::Ephemeral => {
                // Ephemeral events are never stored
                Ok(false)
            }
            EventCategory::Replaceable => self.upsert_replaceable_event(event).await,
            EventCategory::ParameterizedReplaceable => {
                self.upsert_param_replaceable_event(event).await
            }
            EventCategory::Regular => self.insert_event(event).await,
        }
    }

    // ── Reads ──────────────────────────────────────────────────────────────────

    /// Check whether an event with the given ID already exists.
    pub async fn event_exists(&self, id: &str) -> Result<bool> {
        let row = sqlx::query("SELECT 1 FROM events WHERE id = $1 LIMIT 1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .context("event_exists query failed")?;

        Ok(row.is_some())
    }

    /// Fetch events matching a simple filter (used for REQ EOSE delivery).
    ///
    /// The full filter engine lives in the subscription module; here we do a
    /// best-effort DB query to seed a new subscription.
    pub async fn query_events(
        &self,
        authors: Option<&[String]>,
        kinds: Option<&[u32]>,
        since: Option<i64>,
        until: Option<i64>,
        limit: usize,
    ) -> Result<Vec<NostrEvent>> {
        // Build a dynamic query.  sqlx does not yet support fully-dynamic
        // WHERE clauses via macros so we construct the SQL string manually and
        // use numbered bind parameters.
        let mut conditions: Vec<String> = Vec::new();
        let mut param_idx: usize = 1;

        if authors.is_some() {
            conditions.push(format!("pubkey = ANY(${param_idx})"));
            param_idx += 1;
        }
        if kinds.is_some() {
            conditions.push(format!("kind = ANY(${param_idx})"));
            param_idx += 1;
        }
        if since.is_some() {
            conditions.push(format!("created_at >= ${param_idx}"));
            param_idx += 1;
        }
        if until.is_some() {
            conditions.push(format!("created_at <= ${param_idx}"));
            param_idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            r#"
            SELECT id, pubkey, created_at, kind, tags, content, sig
            FROM events
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${param_idx}
            "#
        );

        // Bind parameters in the same order as the placeholders
        let mut q = sqlx::query(&sql);
        if let Some(a) = authors {
            q = q.bind(a);
        }
        if let Some(k) = kinds {
            let k_i32: Vec<i32> = k.iter().map(|&x| x as i32).collect();
            q = q.bind(k_i32);
        }
        if let Some(s) = since {
            q = q.bind(s);
        }
        if let Some(u) = until {
            q = q.bind(u);
        }
        q = q.bind(limit as i64);

        let rows = q
            .fetch_all(&self.pool)
            .await
            .context("query_events fetch_all failed")?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let tags_value: serde_json::Value = row.try_get("tags")?;
            let tags: Vec<Vec<String>> = serde_json::from_value(tags_value)
                .context("failed to deserialize tags from DB")?;

            events.push(NostrEvent {
                id: row.try_get("id")?,
                pubkey: row.try_get("pubkey")?,
                created_at: row.try_get("created_at")?,
                kind: row.try_get::<i32, _>("kind")? as u32,
                tags,
                content: row.try_get("content")?,
                sig: row.try_get("sig")?,
            });
        }

        Ok(events)
    }
}
