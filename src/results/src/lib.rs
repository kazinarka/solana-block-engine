//! Routes bundle results back to the searcher that submitted them.
//!
//! The searcher service registers `uuid -> owner pubkey` on submission and adds
//! a subscriber stream per searcher; the auction publishes outcomes by uuid, and
//! the hub delivers each result only to its owning searcher.
//!
//! Scope: emits the outcomes the engine knows authoritatively — `Accepted`
//! (won the auction, forwarded), `Rejected(WinningBatchBidRejected)` (lost), and
//! `Rejected(SimulationFailure)`. On-chain `Processed`/`Finalized`/`Dropped`
//! results would require confirmation tracking and are not emitted yet.

use std::collections::HashMap;
use std::sync::Mutex;

use jito_protos::bundle::{
    bundle_result::Result as ResultKind, rejected::Reason, Accepted, BundleResult, Rejected,
    SimulationFailure, WinningBatchBidRejected,
};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tonic::Status;

pub type ResultSender = Sender<Result<BundleResult, Status>>;

#[derive(Default)]
pub struct BundleResults {
    /// bundle uuid -> owning searcher pubkey (base58). Removed once a terminal
    /// result is published, bounding the map.
    owners: Mutex<HashMap<String, String>>,
    /// searcher pubkey -> active result streams.
    subscribers: Mutex<HashMap<String, Vec<ResultSender>>>,
}

impl BundleResults {
    pub fn new() -> Self {
        Self::default()
    }

    /// Associate a bundle with the searcher that submitted it.
    pub fn register(&self, uuid: &str, owner: &str) {
        self.owners
            .lock()
            .unwrap()
            .insert(uuid.to_string(), owner.to_string());
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

    pub fn publish_accepted(&self, uuid: &str, slot: u64, validator_identity: String) {
        self.publish(
            uuid,
            ResultKind::Accepted(Accepted {
                slot,
                validator_identity,
            }),
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
        );
    }

    /// Deliver a terminal result to its owning searcher's streams.
    fn publish(&self, uuid: &str, result: ResultKind) {
        // Terminal result -> consume the ownership mapping.
        let Some(owner) = self.owners.lock().unwrap().remove(uuid) else {
            return;
        };
        let msg = BundleResult {
            bundle_id: uuid.to_string(),
            result: Some(result),
        };

        let mut subs = self.subscribers.lock().unwrap();
        if let Some(senders) = subs.get_mut(&owner) {
            // Keep live streams; drop only closed ones (a full stream just
            // misses this message rather than being torn down).
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
