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
