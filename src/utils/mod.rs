//! Utility helpers shared across the crate.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current Unix timestamp in seconds.
pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_secs() as i64
}

/// Generate a simple random-looking client ID from the current time + a
/// thread-local counter.  Good enough for disambiguating concurrent
/// connections without pulling in a UUID dependency.
pub fn new_client_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}-{n}", unix_now())
}

/// Truncate a string to `max_chars` Unicode scalar values, appending `…`
/// if truncated.  Useful for log messages.
pub fn truncate(s: &str, max_chars: usize) -> std::borrow::Cow<'_, str> {
    // Find the byte position of the `max_chars`-th character in a single pass.
    let mut byte_end = None;
    for (idx, (byte_pos, _)) in s.char_indices().enumerate() {
        if idx == max_chars {
            byte_end = Some(byte_pos);
            break;
        }
    }

    match byte_end {
        None => std::borrow::Cow::Borrowed(s), // string is short enough
        Some(pos) => std::borrow::Cow::Owned(format!("{}…", &s[..pos])),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_short_string_unchanged() {
        let s = "hello";
        let result = truncate(s, 10);
        assert_eq!(result, "hello");
    }

    #[test]
    fn truncate_exact_length_unchanged() {
        let s = "hello";
        let result = truncate(s, 5);
        assert_eq!(result, "hello");
    }

    #[test]
    fn truncate_long_string_gets_ellipsis() {
        let s = "hello world";
        let result = truncate(s, 5);
        assert_eq!(result, "hello…");
    }

    #[test]
    fn truncate_multibyte_characters() {
        // Each emoji is multiple bytes but one char
        let s = "🦊🦊🦊🦊🦊";
        let result = truncate(s, 3);
        assert_eq!(result, "🦊🦊🦊…");
    }

    #[test]
    fn truncate_empty_string() {
        let result = truncate("", 5);
        assert_eq!(result, "");
    }

    #[test]
    fn unix_now_is_positive() {
        let ts = unix_now();
        assert!(ts > 0, "unix_now should return a positive timestamp");
    }

    #[test]
    fn unix_now_is_reasonable() {
        let ts = unix_now();
        // After 2020-01-01 (1577836800) and before 2100-01-01 (4102444800)
        assert!(ts > 1_577_836_800, "timestamp is too old");
        assert!(ts < 4_102_444_800, "timestamp is too far in the future");
    }

    #[test]
    fn new_client_id_not_empty() {
        let id = new_client_id();
        assert!(!id.is_empty());
    }

    #[test]
    fn new_client_id_is_unique() {
        let id1 = new_client_id();
        let id2 = new_client_id();
        assert_ne!(id1, id2, "consecutive client IDs must differ");
    }

    #[test]
    fn new_client_id_format() {
        let id = new_client_id();
        // Expected format: "<hex_timestamp>-<counter>"
        assert!(id.contains('-'), "client ID must contain a '-' separator");
    }
}
