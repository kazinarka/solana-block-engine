//! Process-wide counters for the block engine.
//!
//! Metrics are ambient/process-scoped, so they live as module-level atomics with
//! free `inc_*` / `add_*` functions — any crate can record an event without
//! plumbing a handle through its constructors. Read them via [`log_snapshot`]
//! (periodic INFO log) or [`render_prometheus`] (text for a future /metrics
//! endpoint).

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use log::info;

macro_rules! counters {
    ($($(#[$doc:meta])* $name:ident => $metric:literal),* $(,)?) => {
        $(
            static $name: AtomicU64 = AtomicU64::new(0);
        )*

        /// Render all counters in Prometheus text exposition format.
        pub fn render_prometheus() -> String {
            let mut out = String::new();
            $(
                out.push_str(concat!("# TYPE ", $metric, " counter\n"));
                out.push_str(&format!("{} {}\n", $metric, $name.load(Relaxed)));
            )*
            out
        }

        /// Log a one-line snapshot of all counters at INFO.
        pub fn log_snapshot() {
            info!(
                concat!("metrics:" $(, " ", $metric, "={}")*),
                $($name.load(Relaxed)),*
            );
        }
    };
}

counters! {
    BUNDLES_RECEIVED => "bundles_received_total",
    BUNDLES_WON => "bundles_won_total",
    BUNDLES_DROPPED => "bundles_dropped_total",
    PACKETS_RECEIVED => "packets_received_total",
    PACKETS_FORWARDED => "packets_forwarded_total",
    PACKETS_EXPIRED => "packets_expired_total",
    AUTH_CHALLENGES => "auth_challenges_total",
    AUTH_SUCCESS => "auth_success_total",
    AUTH_FAILURES => "auth_failures_total",
    VALIDATOR_SUBSCRIPTIONS => "validator_subscriptions_total",
}

pub fn inc_bundles_received() {
    BUNDLES_RECEIVED.fetch_add(1, Relaxed);
}
pub fn add_bundles_won(n: u64) {
    BUNDLES_WON.fetch_add(n, Relaxed);
}
pub fn add_bundles_dropped(n: u64) {
    BUNDLES_DROPPED.fetch_add(n, Relaxed);
}
pub fn inc_packets_received() {
    PACKETS_RECEIVED.fetch_add(1, Relaxed);
}
pub fn inc_packets_forwarded() {
    PACKETS_FORWARDED.fetch_add(1, Relaxed);
}
pub fn inc_packets_expired() {
    PACKETS_EXPIRED.fetch_add(1, Relaxed);
}
pub fn inc_auth_challenges() {
    AUTH_CHALLENGES.fetch_add(1, Relaxed);
}
pub fn inc_auth_success() {
    AUTH_SUCCESS.fetch_add(1, Relaxed);
}
pub fn inc_auth_failures() {
    AUTH_FAILURES.fetch_add(1, Relaxed);
}
pub fn inc_validator_subscriptions() {
    VALIDATOR_SUBSCRIPTIONS.fetch_add(1, Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_format() {
        inc_bundles_received();
        add_bundles_won(3);
        let out = render_prometheus();
        // Each counter is emitted with a TYPE line and a value line.
        assert!(out.contains("# TYPE bundles_received_total counter"));
        assert!(out.contains("bundles_received_total "));
        assert!(out.contains("bundles_won_total "));
        assert!(out.contains("auth_success_total "));
    }
}
