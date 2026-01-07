use std::sync::atomic::{AtomicU64, Ordering};

static CLIENT_IDENTIFIER: AtomicU64 = AtomicU64::new(0);

pub fn create_identifier() -> u64 {
    CLIENT_IDENTIFIER.fetch_add(1, Ordering::SeqCst)
}
