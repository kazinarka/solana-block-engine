//! Routes bundle results back to the searcher that submitted them.
//!
//! The searcher service registers `uuid -> owner pubkey` on submission and adds
//! a subscriber stream per searcher; the auction and the on-chain tracker
//! publish outcomes by uuid, and the hub delivers each result only to its
//! owning searcher.
//!
//! Results progress through non-terminal states (`Accepted`, `Processed`) and a
//! terminal one (`Rejected`, `Finalized`, `Dropped`). The `uuid -> owner`
//! mapping is kept until a terminal result, and a periodic [`prune`] sweep
//! evicts stale mappings for bundles that never resolve.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use jito_protos::bundle::{
    bundle_result::Result as ResultKind, rejected::Reason, Accepted, BundleResult, Dropped,
    DroppedReason, Finalized, Processed, Rejected, SimulationFailure, WinningBatchBidRejected,
};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tonic::Status;

pub type ResultSender = Sender<Result<BundleResult, Status>>;

struct Owner {
    pubkey: String,
    registered_at: Instant,
}

#[derive(Default)]
pub struct BundleResults {
    /// bundle uuid -> owning searcher (+ registration time for TTL eviction).
    owners: Mutex<HashMap<String, Owner>>,
    /// searcher pubkey -> active result streams.
    subscribers: Mutex<HashMap<String, Vec<ResultSender>>>,
}

impl BundleResults {
    pub fn new() -> Self {
        Self::default()
    }

    /// Associate a bundle with the searcher that submitted it.
    pub fn register(&self, uuid: &str, owner: &str) {
        self.owners.lock().unwrap().insert(
            uuid.to_string(),
            Owner {
                pubkey: owner.to_string(),
                registered_at: Instant::now(),
            },
        );
    }

    /// Add a result stream for a searcher.
    pub fn add_subscriber(&self, owner: &str, sender: ResultSender) {
        self.subscribers
            .lock()
            .unwrap()
            .entry(owner.to_string())
            .or_default()
            .push(sender);
    }

    /// Evict `uuid -> owner` mappings older than `ttl` (bundles that never
    /// reached a terminal result).
    pub fn prune(&self, ttl: Duration) {
        let now = Instant::now();
        self.owners
            .lock()
            .unwrap()
            .retain(|_, o| now.duration_since(o.registered_at) < ttl);
    }

    // --- non-terminal results (mapping retained) ---

    pub fn publish_accepted(&self, uuid: &str, slot: u64, validator_identity: String) {
        self.publish(
            uuid,
            ResultKind::Accepted(Accepted {
                slot,
                validator_identity,
            }),
            false,
        );
    }

    pub fn publish_processed(&self, uuid: &str, slot: u64, validator_identity: String) {
        self.publish(
            uuid,
            ResultKind::Processed(Processed {
                validator_identity,
                slot,
                bundle_index: 0,
            }),
            false,
        );
    }

    // --- terminal results (mapping consumed) ---

    pub fn publish_finalized(&self, uuid: &str) {
        self.publish(uuid, ResultKind::Finalized(Finalized {}), true);
    }

    pub fn publish_dropped(&self, uuid: &str, reason: DroppedReason) {
        self.publish(
            uuid,
            ResultKind::Dropped(Dropped {
                reason: reason as i32,
            }),
            true,
        );
    }

    pub fn publish_lost_auction(&self, uuid: &str, auction_id: String, bid_lamports: u64) {
        self.publish(
            uuid,
            ResultKind::Rejected(Rejected {
                reason: Some(Reason::WinningBatchBidRejected(WinningBatchBidRejected {
                    auction_id,
                    simulated_bid_lamports: bid_lamports,
                    msg: None,
                })),
            }),
            true,
        );
    }

    pub fn publish_sim_failure(&self, uuid: &str, msg: String) {
        self.publish(
            uuid,
            ResultKind::Rejected(Rejected {
                reason: Some(Reason::SimulationFailure(SimulationFailure {
                    tx_signature: String::new(),
                    msg: Some(msg),
                })),
            }),
            true,
        );
    }

    /// Deliver a result to its owning searcher's streams. When `terminal`, the
    /// ownership mapping is consumed so no further results are delivered.
    fn publish(&self, uuid: &str, result: ResultKind, terminal: bool) {
        let owner = {
            let mut owners = self.owners.lock().unwrap();
            if terminal {
                owners.remove(uuid).map(|o| o.pubkey)
            } else {
                owners.get(uuid).map(|o| o.pubkey.clone())
            }
        };
        let Some(owner) = owner else {
            return;
        };
        let msg = BundleResult {
            bundle_id: uuid.to_string(),
            result: Some(result),
        };

        let mut subs = self.subscribers.lock().unwrap();
        if let Some(senders) = subs.get_mut(&owner) {
            senders.retain(|s| match s.try_send(Ok(msg.clone())) {
                Ok(_) | Err(TrySendError::Full(_)) => true,
                Err(TrySendError::Closed(_)) => false,
            });
            if senders.is_empty() {
                subs.remove(&owner);
            }
        }
    }
}
