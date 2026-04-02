# neon-fox-relay

A high-performance [Nostr](https://nostr.com/) relay server built in Rust.

## Features

- **NIP-01** – Base protocol (EVENT, REQ, CLOSE, OK, EOSE, NOTICE)
- **NIP-11** – Relay information document (`/relay-info`)
- **NIP-16** – Replaceable events (kinds 0, 3, 10000–19999 upserted by pubkey+kind)
- **NIP-33** – Parameterized replaceable events (kinds 30000–39999 upserted by pubkey+kind+d-tag)
- **Ephemeral events** – kinds 20000–29999 are broadcast in real-time but never persisted
- **In-memory subscription matching** – hot-path fanout with zero database I/O
- **Redis write queue** – events are enqueued via Redis Streams and persisted by a background worker (at-least-once delivery)
- **Redis event cache** – recent events are cached for fast reads
- **Graceful shutdown** – handles SIGTERM and Ctrl-C

## Tech Stack

| Component | Library |
|-----------|---------|
| Async runtime | [tokio](https://tokio.rs/) |
| HTTP / WebSocket | [axum](https://github.com/tokio-rs/axum) |
| PostgreSQL | [sqlx](https://github.com/launchbadge/sqlx) |
| Redis | [redis-rs](https://github.com/redis-rs/redis-rs) |
| Schnorr signatures | [secp256k1](https://github.com/rust-bitcoin/rust-secp256k1) |
| Serialisation | [serde_json](https://github.com/serde-rs/json) |
| Logging | [tracing](https://github.com/tokio-rs/tracing) |

## Quick Start

### Prerequisites

- Rust (stable)
- PostgreSQL
- Redis

### Configuration

Copy `.env.example` to `.env` and fill in your values:

```sh
cp .env.example .env
```

| Variable | Default | Description |
|----------|---------|-------------|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Bind port |
| `DATABASE_URL` | *(required)* | PostgreSQL connection string |
| `DATABASE_MAX_CONNECTIONS` | `20` | PG connection pool size |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL |
| `RELAY_NAME` | `neon-fox-relay` | Name shown in NIP-11 |
| `RELAY_DESCRIPTION` | | Description shown in NIP-11 |
| `RELAY_PUBKEY` | | Relay operator pubkey (hex) |
| `RELAY_CONTACT` | | Operator contact (email / nostr address) |
| `WRITE_STREAM_KEY` | `nostr:events:stream` | Redis stream key for the write queue |
| `WORKER_GROUP` | `nostr-workers` | Redis consumer group name |
| `WORKER_CONSUMER` | `worker-1` | Redis consumer name for this instance |
| `MAX_EVENT_TAGS` | `2000` | Maximum tags per event |
| `MAX_CONTENT_LENGTH` | `65536` | Maximum content length in bytes |
| `MAX_SUBSCRIPTIONS_PER_CLIENT` | `20` | Maximum open subscriptions per client |
| `MAX_FILTERS_PER_SUBSCRIPTION` | `10` | Maximum filters per REQ message |

### Database Migration

```sh
psql "$DATABASE_URL" -f migrations/001_initial_schema.sql
```

### Run

```sh
cargo run --release
```

## Architecture

```
WebSocket clients
       │
       ▼
  axum router (/)
       │
       ▼
 handle_socket  ─────── outbound mpsc channel ──────► ws_sender task
       │
       ▼
 dispatch_message
  ├── EVENT ──► handle_event (service)
  │               ├── validate (ID hash + Schnorr sig)
  │               ├── ephemeral ──► Redis PUBLISH
  │               ├── non-ephemeral ──► Redis XADD (stream)
  │               └── broadcast to matching local subscriptions
  ├── REQ ────► register subscription + query historical events + EOSE
  └── CLOSE ──► remove subscription

Redis Stream (nostr:events:stream)
       │
       ▼
 background worker
       │
       ▼
  PostgreSQL (events table)
```

## Endpoints

| Path | Purpose |
|------|---------|
| `ws://host/` | Nostr WebSocket endpoint |
| `GET /health` | Health check – returns `200 OK` |
| `GET /relay-info` | NIP-11 relay information JSON |

## Development

```sh
# Check compilation
cargo check

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run
```

## License

MIT
