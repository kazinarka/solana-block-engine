//! Derives the engine's "accounts of interest" (AOI) and "programs of interest"
//! (POI) from submitted bundles, so the relayer only forwards transactions that
//! touch contended state instead of the entire packet flow.
//!
//! When a searcher submits a bundle, every *writable* account it references and
//! every program it invokes is recorded (with a TTL). The relayer service
//! streams the current sets to the relayer.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bincode::deserialize;
use jito_protos::bundle::BundleUuid;
use jito_protos::packet::Packet;
use solana_sdk::transaction::VersionedTransaction;

pub struct InterestRegistry {
    /// base58 account pubkey -> last time it appeared (writable) in a bundle.
    accounts: Mutex<HashMap<String, Instant>>,
    /// base58 program id -> last time it was invoked by a bundle.
    programs: Mutex<HashMap<String, Instant>>,
    ttl: Duration,
}

impl InterestRegistry {
    pub fn new(ttl: Duration) -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
            programs: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Record the writable accounts and invoked programs of a bundle.
    pub fn observe_bundle(&self, bundle: &BundleUuid) {
        let packets: &[Packet] = bundle
            .bundle
            .as_ref()
            .map(|b| b.packets.as_slice())
            .unwrap_or(&[]);

        let now = Instant::now();
        let mut accounts = self.accounts.lock().unwrap();
        let mut programs = self.programs.lock().unwrap();

        for packet in packets {
            let Ok(tx) = deserialize::<VersionedTransaction>(&packet.data) else {
                continue;
            };
            let msg = &tx.message;
            let keys = msg.static_account_keys();
            let header = msg.header();
            let n = keys.len();
            let num_signed = header.num_required_signatures as usize;
            let num_ro_signed = header.num_readonly_signed_accounts as usize;
            let num_ro_unsigned = header.num_readonly_unsigned_accounts as usize;

            for (i, key) in keys.iter().enumerate() {
                if is_writable_static(i, n, num_signed, num_ro_signed, num_ro_unsigned) {
                    accounts.insert(key.to_string(), now);
                }
            }

            for ix in msg.instructions() {
                if let Some(program_id) = keys.get(ix.program_id_index as usize) {
                    programs.insert(program_id.to_string(), now);
                }
            }
        }
    }

    pub fn accounts_of_interest(&self) -> Vec<String> {
        Self::collect(&self.accounts, self.ttl)
    }

    pub fn programs_of_interest(&self) -> Vec<String> {
        Self::collect(&self.programs, self.ttl)
    }

    /// Drop entries older than the TTL (bounds memory).
    pub fn prune(&self) {
        let now = Instant::now();
        self.accounts
            .lock()
            .unwrap()
            .retain(|_, t| now.duration_since(*t) < self.ttl);
        self.programs
            .lock()
            .unwrap()
            .retain(|_, t| now.duration_since(*t) < self.ttl);
    }

    fn collect(map: &Mutex<HashMap<String, Instant>>, ttl: Duration) -> Vec<String> {
        let now = Instant::now();
        map.lock()
            .unwrap()
            .iter()
            .filter(|(_, t)| now.duration_since(**t) < ttl)
            .map(|(k, _)| k.clone())
            .collect()
    }
}

/// Whether the static account key at `index` is writable, per the Solana message
/// header layout: signed accounts come first (the last `num_ro_signed` of them
/// are read-only), then unsigned accounts (the last `num_ro_unsigned` read-only).
fn is_writable_static(
    index: usize,
    num_accounts: usize,
    num_signed: usize,
    num_ro_signed: usize,
    num_ro_unsigned: usize,
) -> bool {
    if index < num_signed {
        index < num_signed.saturating_sub(num_ro_signed)
    } else {
        index < num_accounts.saturating_sub(num_ro_unsigned)
    }
}

#[cfg(test)]
mod tests {
    use super::is_writable_static;

    #[test]
    fn classifies_each_account_region() {
        assert!(is_writable_static(0, 4, 2, 1, 1));
        assert!(!is_writable_static(1, 4, 2, 1, 1));
        assert!(is_writable_static(2, 4, 2, 1, 1));
        assert!(!is_writable_static(3, 4, 2, 1, 1));
    }

    #[test]
    fn all_writable() {
        assert!(is_writable_static(0, 2, 1, 0, 0));
        assert!(is_writable_static(1, 2, 1, 0, 0));
    }

    #[test]
    fn all_readonly() {
        assert!(!is_writable_static(0, 2, 1, 1, 1));
        assert!(!is_writable_static(1, 2, 1, 1, 1));
    }
}
