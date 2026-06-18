//! Tracks forwarded bundles on-chain and reports their fate to searchers.
//!
//! When a bundle wins the auction and is forwarded, [`BundleTracker::track`] is
//! called with its transaction signatures. A background task polls
//! `getSignatureStatuses` and publishes, via the results hub:
//!   * `Processed`  — once all the bundle's transactions are confirmed,
//!   * `Finalized`  — once they reach finalized commitment (terminal),
//!   * `Dropped(BlockhashExpired)` — if they don't land within the deadline.
//!
//! A bundle is atomic, so it's only considered landed when *all* its signatures
//! are observed at the relevant commitment.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bincode::deserialize;
use jito_protos::bundle::{BundleUuid, DroppedReason};
use jito_protos::packet::Packet;
use jito_results::BundleResults;
use log::{debug, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status_client_types::TransactionConfirmationStatus;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Max signatures per `getSignatureStatuses` call.
const MAX_SIGS_PER_CALL: usize = 256;

struct Tracked {
    uuid: String,
    signatures: Vec<Signature>,
    tracked_since: Instant,
    processed_emitted: bool,
}

pub struct BundleTracker {
    rpc: RpcClient,
    results: Arc<BundleResults>,
    pending: Mutex<Vec<Tracked>>,
    /// How long to wait for a bundle to land before declaring it dropped.
    deadline: Duration,
}

impl BundleTracker {
    pub fn start(rpc_url: String, results: Arc<BundleResults>, deadline: Duration) -> Arc<Self> {
        let tracker = Arc::new(Self {
            rpc: RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed()),
            results,
            pending: Mutex::new(Vec::new()),
            deadline,
        });
        let t = tracker.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(POLL_INTERVAL);
            loop {
                interval.tick().await;
                t.poll().await;
            }
        });
        tracker
    }

    /// Begin tracking a forwarded bundle by its transaction signatures.
    pub fn track(&self, bundle: &BundleUuid) {
        let signatures: Vec<Signature> = packets(bundle)
            .iter()
            .filter_map(primary_signature)
            .collect();
        if signatures.is_empty() {
            return;
        }
        self.pending.lock().unwrap().push(Tracked {
            uuid: bundle.uuid.clone(),
            signatures,
            tracked_since: Instant::now(),
            processed_emitted: false,
        });
    }

    async fn poll(&self) {
        // Collect a bounded, de-duplicated batch of signatures to query.
        let all_sigs: Vec<Signature> = {
            let pending = self.pending.lock().unwrap();
            let mut sigs: Vec<Signature> =
                pending.iter().flat_map(|t| t.signatures.clone()).collect();
            sigs.sort();
            sigs.dedup();
            sigs.truncate(MAX_SIGS_PER_CALL);
            sigs
        };
        if all_sigs.is_empty() {
            return;
        }

        let statuses = match self.rpc.get_signature_statuses(&all_sigs).await {
            Ok(resp) => resp.value,
            Err(e) => {
                warn!("tracker: get_signature_statuses failed: {e}");
                return;
            }
        };

        // sig -> (confirmation_status, slot)
        let mut status_by_sig = std::collections::HashMap::new();
        for (sig, status) in all_sigs.iter().zip(statuses) {
            if let Some(s) = status {
                status_by_sig.insert(*sig, (s.confirmation_status, s.slot));
            }
        }

        let mut to_remove = Vec::new();
        let mut to_finalize = Vec::new();
        let mut to_process = Vec::new();
        let mut to_drop = Vec::new();

        {
            let mut pending = self.pending.lock().unwrap();
            for (idx, t) in pending.iter_mut().enumerate() {
                let resolved: Vec<_> = t
                    .signatures
                    .iter()
                    .map(|s| status_by_sig.get(s))
                    .collect();

                let all_finalized = resolved.iter().all(|r| {
                    matches!(
                        r,
                        Some((Some(TransactionConfirmationStatus::Finalized), _))
                    )
                });
                let all_confirmed = resolved.iter().all(|r| {
                    matches!(
                        r,
                        Some((
                            Some(
                                TransactionConfirmationStatus::Confirmed
                                    | TransactionConfirmationStatus::Finalized
                            ),
                            _
                        ))
                    )
                });
                let slot = resolved
                    .iter()
                    .filter_map(|r| r.map(|(_, slot)| *slot))
                    .max()
                    .unwrap_or(0);

                if all_finalized {
                    to_finalize.push(t.uuid.clone());
                    to_remove.push(idx);
                } else if all_confirmed && !t.processed_emitted {
                    t.processed_emitted = true;
                    to_process.push((t.uuid.clone(), slot));
                } else if t.tracked_since.elapsed() > self.deadline {
                    to_drop.push(t.uuid.clone());
                    to_remove.push(idx);
                }
            }
            // Remove resolved/expired entries (descending index to keep validity).
            to_remove.sort_unstable();
            to_remove.dedup();
            for idx in to_remove.into_iter().rev() {
                pending.remove(idx);
            }
        }

        for (uuid, slot) in to_process {
            debug!("tracker: bundle {uuid} processed at slot {slot}");
            self.results.publish_processed(&uuid, slot, String::new());
        }
        for uuid in to_finalize {
            debug!("tracker: bundle {uuid} finalized");
            self.results.publish_finalized(&uuid);
        }
        for uuid in to_drop {
            debug!("tracker: bundle {uuid} dropped (never landed)");
            self.results
                .publish_dropped(&uuid, DroppedReason::BlockhashExpired);
        }
    }
}

fn packets(bundle: &BundleUuid) -> &[Packet] {
    bundle
        .bundle
        .as_ref()
        .map(|b| b.packets.as_slice())
        .unwrap_or(&[])
}

/// The first (fee-payer) signature identifies a transaction on-chain.
fn primary_signature(packet: &Packet) -> Option<Signature> {
    let tx: VersionedTransaction = deserialize(&packet.data).ok()?;
    tx.signatures.first().copied()
}
