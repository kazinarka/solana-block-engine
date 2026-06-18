//! Tracks the Solana leader schedule so the block engine can forward packets and
//! bundles only to the validator that is currently (or imminently) leader,
//! instead of fanning out to every connected validator.
//!
//! Source of truth is a Solana RPC node:
//!   * `getEpochInfo`     — current absolute slot + position within the epoch
//!   * `getLeaderSchedule`— identity pubkey → relative slot indices for the epoch
//!
//! The leader schedule is fixed for an epoch, so we fetch it once per epoch and
//! then just poll the current slot frequently, recomputing a small "upcoming
//! leaders" set for the lookahead window. The forwarding hot path does an O(1)
//! membership check against that set.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use log::{info, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_commitment_config::CommitmentConfig;

/// How often to poll the current slot.
const POLL_INTERVAL: Duration = Duration::from_millis(400);

pub struct LeaderTracker {
    current_slot: AtomicU64,
    /// Base58 identities scheduled to lead within the lookahead window.
    upcoming: RwLock<HashSet<String>>,
}

impl LeaderTracker {
    /// Start a background task that keeps the tracker fresh. Must be called from
    /// within a tokio runtime. `lookahead_slots` is how far ahead of the current
    /// slot a validator is considered "upcoming leader" (so packets arrive
    /// before its slot starts).
    pub fn start(rpc_url: String, lookahead_slots: u64) -> Arc<Self> {
        let tracker = Arc::new(Self {
            current_slot: AtomicU64::new(0),
            upcoming: RwLock::new(HashSet::new()),
        });

        let t = tracker.clone();
        tokio::spawn(async move {
            let rpc = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed());
            // Cached schedule for the current epoch: absolute slot -> identity.
            let mut schedule: HashMap<u64, String> = HashMap::new();
            let mut loaded_epoch: Option<u64> = None;
            let mut interval = tokio::time::interval(POLL_INTERVAL);

            loop {
                interval.tick().await;

                let epoch_info = match rpc.get_epoch_info().await {
                    Ok(e) => e,
                    Err(e) => {
                        warn!("leader-tracker: get_epoch_info failed: {e}");
                        continue;
                    }
                };
                let abs_slot = epoch_info.absolute_slot;
                let epoch_start = abs_slot.saturating_sub(epoch_info.slot_index);
                t.current_slot.store(abs_slot, Ordering::Relaxed);

                // (Re)load the schedule when the epoch rolls over.
                if loaded_epoch != Some(epoch_info.epoch) {
                    match rpc.get_leader_schedule(Some(abs_slot)).await {
                        Ok(Some(map)) => {
                            schedule.clear();
                            for (identity, rel_slots) in map {
                                for rel in rel_slots {
                                    schedule.insert(epoch_start + rel as u64, identity.clone());
                                }
                            }
                            loaded_epoch = Some(epoch_info.epoch);
                            info!(
                                "leader-tracker: loaded schedule for epoch {} ({} slots)",
                                epoch_info.epoch,
                                schedule.len()
                            );
                        }
                        Ok(None) => warn!("leader-tracker: RPC returned no leader schedule"),
                        Err(e) => warn!("leader-tracker: get_leader_schedule failed: {e}"),
                    }
                }

                // Recompute the upcoming-leaders window.
                let mut set = HashSet::new();
                for slot in abs_slot..=abs_slot.saturating_add(lookahead_slots) {
                    if let Some(identity) = schedule.get(&slot) {
                        set.insert(identity.clone());
                    }
                }
                *t.upcoming.write().unwrap() = set;
            }
        });

        tracker
    }

    /// Is this validator identity (base58) leading within the lookahead window?
    pub fn is_upcoming_leader(&self, identity_b58: &str) -> bool {
        self.upcoming.read().unwrap().contains(identity_b58)
    }

    pub fn current_slot(&self) -> Slot {
        self.current_slot.load(Ordering::Relaxed)
    }
}
