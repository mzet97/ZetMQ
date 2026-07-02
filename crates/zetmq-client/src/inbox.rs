use std::sync::atomic::{AtomicU64, Ordering};

static INBOX_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique inbox prefix: `_INBOX.<random_hex>.<counter>`
pub fn generate_inbox_prefix() -> String {
    let rand_part: u64 = rand::random();
    let counter = INBOX_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("_INBOX.{:016x}.{}", rand_part, counter)
}
