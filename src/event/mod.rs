//! Event module – types, validation, ID computation, and signature verification.
//!
//! Implements the core of NIP-01, NIP-16 and NIP-33:
//!   - Event structure and de/serialisation
//!   - Event-ID computation (SHA-256 of the canonical serialisation)
//!   - Schnorr signature verification via `secp256k1`
//!   - Event classification (normal / ephemeral / replaceable / parameterized-replaceable)

use anyhow::{bail, Context, Result};
use secp256k1::{schnorr::Signature as SchnorrSig, Message, Secp256k1, XOnlyPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ── Event struct ──────────────────────────────────────────────────────────────

/// A Nostr event as defined by NIP-01.
///
/// All hex strings are lowercase.  `tags` is a list of tag arrays where the
/// first element is the tag name and the rest are the tag values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NostrEvent {
    /// 32-byte lowercase hex SHA-256 of the serialised event
    pub id: String,
    /// 32-byte lowercase hex public key of the event creator
    pub pubkey: String,
    /// Unix timestamp (seconds)
    pub created_at: i64,
    /// Event kind
    pub kind: u32,
    /// Ordered list of tags, each tag is `[name, value, ...]`
    pub tags: Vec<Vec<String>>,
    /// Arbitrary UTF-8 string
    pub content: String,
    /// 64-byte lowercase hex Schnorr signature of `id`
    pub sig: String,
}

// ── Kind classification ───────────────────────────────────────────────────────

/// Describes how an event should be stored / routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventCategory {
    /// Sent over Redis pub/sub but never stored (kinds 20000–29999)
    Ephemeral,
    /// Only one event per (pubkey, kind) is kept – upsert (kinds 0, 3, 10000–19999)
    Replaceable,
    /// Only one event per (pubkey, kind, d-tag) is kept – upsert (kinds 30000–39999, NIP-33)
    ParameterizedReplaceable,
    /// Stored as-is (everything else)
    Regular,
}

impl NostrEvent {
    /// Classify this event according to NIP-16 / NIP-33.
    pub fn category(&self) -> EventCategory {
        match self.kind {
            0 | 3 => EventCategory::Replaceable,
            10_000..=19_999 => EventCategory::Replaceable,
            20_000..=29_999 => EventCategory::Ephemeral,
            30_000..=39_999 => EventCategory::ParameterizedReplaceable,
            _ => EventCategory::Regular,
        }
    }

    /// Return `true` if this event should *not* be persisted.
    pub fn is_ephemeral(&self) -> bool {
        self.category() == EventCategory::Ephemeral
    }

    /// Return `true` if this event should replace a previous one.
    pub fn is_replaceable(&self) -> bool {
        matches!(
            self.category(),
            EventCategory::Replaceable | EventCategory::ParameterizedReplaceable
        )
    }

    /// For parameterized-replaceable events (NIP-33) return the value of the
    /// first `d` tag, or `""` when absent.
    pub fn d_tag(&self) -> &str {
        for tag in &self.tags {
            if tag.first().map(|s| s.as_str()) == Some("d") {
                return tag.get(1).map(|s| s.as_str()).unwrap_or("");
            }
        }
        ""
    }

    // ── Validation ────────────────────────────────────────────────────────────

    /// Fully validate the event:
    /// 1. Check required fields are present and well-formed
    /// 2. Recompute the event ID and compare
    /// 3. Verify the Schnorr signature
    pub fn validate(&self) -> Result<()> {
        validate_hex_field("id", &self.id, 64)?;
        validate_hex_field("pubkey", &self.pubkey, 64)?;
        validate_hex_field("sig", &self.sig, 128)?;

        // Verify event ID
        let expected_id = compute_event_id(
            &self.pubkey,
            self.created_at,
            self.kind,
            &self.tags,
            &self.content,
        )?;
        if expected_id != self.id {
            bail!("invalid event id: computed {expected_id}, got {}", self.id);
        }

        // Verify Schnorr signature
        verify_signature(&self.id, &self.pubkey, &self.sig)
            .context("signature verification failed")?;

        Ok(())
    }
}

// ── ID computation ────────────────────────────────────────────────────────────

/// Compute the canonical NIP-01 event ID.
///
/// The ID is the SHA-256 of the JSON-serialised array
/// `[0, pubkey, created_at, kind, tags, content]`
/// with no extra whitespace.
pub fn compute_event_id(
    pubkey: &str,
    created_at: i64,
    kind: u32,
    tags: &[Vec<String>],
    content: &str,
) -> Result<String> {
    // Build the canonical serialisation array
    let serialised = serde_json::to_string(&serde_json::json!([
        0,
        pubkey,
        created_at,
        kind,
        tags,
        content,
    ]))
    .context("failed to serialise event for ID computation")?;

    let mut hasher = Sha256::new();
    hasher.update(serialised.as_bytes());
    let hash = hasher.finalize();

    Ok(hex::encode(hash))
}

// ── Signature verification ────────────────────────────────────────────────────

/// Verify a NIP-01 Schnorr signature.
///
/// * `event_id_hex` – 64-char lowercase hex (= the 32-byte message digest)
/// * `pubkey_hex`   – 64-char lowercase hex x-only public key
/// * `sig_hex`      – 128-char lowercase hex Schnorr signature
pub fn verify_signature(event_id_hex: &str, pubkey_hex: &str, sig_hex: &str) -> Result<()> {
    let id_bytes = hex::decode(event_id_hex).context("invalid event_id hex")?;
    let pk_bytes = hex::decode(pubkey_hex).context("invalid pubkey hex")?;
    let sig_bytes = hex::decode(sig_hex).context("invalid sig hex")?;

    if id_bytes.len() != 32 {
        bail!("event id must be 32 bytes");
    }
    if pk_bytes.len() != 32 {
        bail!("pubkey must be 32 bytes");
    }
    if sig_bytes.len() != 64 {
        bail!("signature must be 64 bytes");
    }

    let secp = Secp256k1::verification_only();

    let pubkey = XOnlyPublicKey::from_slice(&pk_bytes).context("invalid public key")?;
    let sig = SchnorrSig::from_slice(&sig_bytes).context("invalid schnorr signature")?;

    // secp256k1::Message wraps a 32-byte digest
    let digest: [u8; 32] = id_bytes.try_into().unwrap();
    let msg = Message::from_digest_slice(&digest).context("failed to build secp256k1 Message")?;

    secp.verify_schnorr(&sig, &msg, &pubkey)
        .context("schnorr verification failed")?;

    Ok(())
}

// ── Helper ────────────────────────────────────────────────────────────────────

/// Check that `value` is a valid lowercase hex string of exactly `expected_len` chars.
fn validate_hex_field(name: &str, value: &str, expected_len: usize) -> Result<()> {
    if value.len() != expected_len {
        bail!(
            "field `{name}` must be {expected_len} hex chars, got {}",
            value.len()
        );
    }
    if !value.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("field `{name}` contains non-hex characters");
    }
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_category() {
        let make = |kind| NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at: 0,
            kind,
            tags: vec![],
            content: String::new(),
            sig: "c".repeat(128),
        };

        assert_eq!(make(1).category(), EventCategory::Regular);
        assert_eq!(make(0).category(), EventCategory::Replaceable);
        assert_eq!(make(3).category(), EventCategory::Replaceable);
        assert_eq!(make(10000).category(), EventCategory::Replaceable);
        assert_eq!(make(20000).category(), EventCategory::Ephemeral);
        assert_eq!(make(30000).category(), EventCategory::ParameterizedReplaceable);
    }

    #[test]
    fn test_d_tag_extraction() {
        let event = NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at: 0,
            kind: 30000,
            tags: vec![
                vec!["e".to_string(), "some_id".to_string()],
                vec!["d".to_string(), "my-identifier".to_string()],
            ],
            content: String::new(),
            sig: "c".repeat(128),
        };
        assert_eq!(event.d_tag(), "my-identifier");
    }

    #[test]
    fn test_compute_event_id_is_deterministic() {
        let id1 = compute_event_id("pubkey", 1000, 1, &[], "hello").unwrap();
        let id2 = compute_event_id("pubkey", 1000, 1, &[], "hello").unwrap();
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 64);
    }

    #[test]
    fn test_validate_hex_field_ok() {
        assert!(validate_hex_field("id", &"a".repeat(64), 64).is_ok());
    }

    #[test]
    fn test_validate_hex_field_wrong_length() {
        assert!(validate_hex_field("id", "abc", 64).is_err());
    }
}
