//! Bundle auction: buffers incoming bundles, scores each by the tip it pays,
//! and selects the highest-value set that fits the block's compute-unit budget.
//!
//! This replaces the reference engine's "forward every bundle immediately"
//! behaviour. Searchers submit bundles, which accumulate in a buffer; on each
//! auction tick the engine runs [`Auction::run_auction`], emits the winners to
//! the validator, and drops the rest.
//!
//! ## What's real vs estimated here (step 4a)
//! * **Tip** is extracted for real: we decode each transaction and sum lamports
//!   transferred (via SystemProgram) to any configured tip account.
//! * **Compute units** are *estimated* (`EST_CU_PER_TX` per transaction). Real
//!   per-bundle CU comes from simulation (step 4b); until then the CU budget is
//!   a coarse packing bound.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bincode::deserialize;
use jito_protos::bundle::BundleUuid;
use jito_results::BundleResults;
use jito_protos::packet::Packet;
use log::{debug, info};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction::SystemInstruction;
use solana_sdk::system_program;
use solana_sdk::transaction::VersionedTransaction;

/// Rough per-transaction compute-unit estimate, used for bundles that haven't
/// been simulated yet (or when no simulator is configured).
pub const EST_CU_PER_TX: u64 = 200_000;

/// Result of simulating a bundle against cluster state.
#[derive(Clone, Copy, Debug)]
pub struct SimOutcome {
    /// Did every transaction in the bundle execute without error?
    pub ok: bool,
    /// Total compute units consumed by the bundle.
    pub units_consumed: u64,
}

/// A bundle waiting in the auction, scored at submission time.
struct PendingBundle {
    bundle: BundleUuid,
    tip_lamports: u64,
    est_cu: u64,
    received: Instant,
    /// `None` until simulated. `Some` carries the real CU + success.
    sim: Option<SimOutcome>,
}

impl PendingBundle {
    /// CU to charge against the block budget: real if simulated, else estimate.
    fn cu(&self) -> u64 {
        self.sim.map(|s| s.units_consumed).unwrap_or(self.est_cu)
    }

    /// Drop only if simulation ran and the bundle failed.
    fn failed_simulation(&self) -> bool {
        matches!(self.sim, Some(s) if !s.ok)
    }
}

pub struct Auction {
    buffer: Mutex<Vec<PendingBundle>>,
    tip_accounts: HashSet<Pubkey>,
    /// Per-block compute-unit ceiling the winning set must fit within.
    cu_budget: u64,
    /// Bundles older than this are dropped before each auction.
    bundle_ttl: Duration,
    /// Optional sink for per-bundle results delivered back to searchers.
    results: Option<Arc<BundleResults>>,
}

impl Auction {
    pub fn new(tip_accounts: HashSet<Pubkey>, cu_budget: u64, bundle_ttl: Duration) -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
            tip_accounts,
            cu_budget,
            bundle_ttl,
            results: None,
        }
    }

    /// Attach a results sink so auction outcomes are reported to searchers.
    pub fn with_results(mut self, results: Arc<BundleResults>) -> Self {
        self.results = Some(results);
        self
    }

    /// Build from CLI-style config: parse base58 tip accounts (invalid ones are
    /// logged and skipped).
    pub fn from_config(tip_accounts_b58: &[String], cu_budget: u64, bundle_ttl: Duration) -> Self {
        let mut set = HashSet::new();
        for s in tip_accounts_b58 {
            match Pubkey::from_str(s) {
                Ok(pk) => {
                    set.insert(pk);
                }
                Err(_) => log::warn!("auction: ignoring invalid tip account '{s}'"),
            }
        }
        if set.is_empty() {
            log::warn!("auction: no tip accounts configured; all bundle tips score as 0");
        } else {
            info!("auction: {} tip account(s) configured", set.len());
        }
        Self::new(set, cu_budget, bundle_ttl)
    }

    /// Score and buffer a bundle submitted by a searcher.
    pub fn submit(&self, bundle: BundleUuid) {
        let (tip_lamports, est_cu) = self.score(&bundle);
        debug!(
            "auction: buffered bundle {} tip={} est_cu={}",
            bundle.uuid, tip_lamports, est_cu
        );
        jito_metrics::inc_bundles_received();
        self.buffer.lock().unwrap().push(PendingBundle {
            bundle,
            tip_lamports,
            est_cu,
            received: Instant::now(),
            sim: None,
        });
    }

    /// Bundles awaiting simulation (uuid + a clone of the bundle). The caller
    /// simulates them off-lock, then reports back via [`set_simulation`].
    pub fn pending_for_simulation(&self) -> Vec<(String, BundleUuid)> {
        self.buffer
            .lock()
            .unwrap()
            .iter()
            .filter(|b| b.sim.is_none())
            .map(|b| (b.bundle.uuid.clone(), b.bundle.clone()))
            .collect()
    }

    /// Record a simulation result against a buffered bundle (matched by uuid).
    pub fn set_simulation(&self, uuid: &str, outcome: SimOutcome) {
        if let Some(b) = self
            .buffer
            .lock()
            .unwrap()
            .iter_mut()
            .find(|b| b.bundle.uuid == uuid)
        {
            b.sim = Some(outcome);
        }
        if !outcome.ok {
            if let Some(r) = &self.results {
                r.publish_sim_failure(uuid, "bundle failed simulation".to_string());
            }
        }
    }

    /// Compute (total_tip_lamports, estimated_cu) for a bundle.
    fn score(&self, bundle: &BundleUuid) -> (u64, u64) {
        let packets: &[Packet] = bundle
            .bundle
            .as_ref()
            .map(|b| b.packets.as_slice())
            .unwrap_or(&[]);

        let mut tip = 0u64;
        for packet in packets {
            if let Some(tx) = decode_tx(packet) {
                tip = tip.saturating_add(self.tx_tip(&tx));
            }
        }
        let est_cu = (packets.len() as u64).saturating_mul(EST_CU_PER_TX);
        (tip, est_cu)
    }

    /// Sum lamports transferred to any configured tip account within a tx.
    fn tx_tip(&self, tx: &VersionedTransaction) -> u64 {
        let msg = &tx.message;
        let keys = msg.static_account_keys();
        let mut tip = 0u64;
        for ix in msg.instructions() {
            // Only SystemProgram transfers can pay a tip in lamports.
            if keys.get(ix.program_id_index as usize) != Some(&system_program::id()) {
                continue;
            }
            if let Ok(SystemInstruction::Transfer { lamports }) =
                deserialize::<SystemInstruction>(&ix.data)
            {
                // Transfer's destination is the 2nd instruction account.
                if let Some(&dest_idx) = ix.accounts.get(1) {
                    if let Some(dest) = keys.get(dest_idx as usize) {
                        if self.tip_accounts.contains(dest) {
                            tip = tip.saturating_add(lamports);
                        }
                    }
                }
            }
        }
        tip
    }

    /// Run one auction: drop expired bundles, rank the rest by tip-per-CU, and
    /// return the winning set that fits the CU budget. Clears the buffer.
    pub fn run_auction(&self) -> Vec<BundleUuid> {
        let mut buf = self.buffer.lock().unwrap();
        if buf.is_empty() {
            return Vec::new();
        }
        let entered = buf.len();

        let now = Instant::now();
        // Drop expired bundles and any that failed simulation (they'd fail
        // on-chain too, so they shouldn't take up block space).
        buf.retain(|b| {
            now.duration_since(b.received) < self.bundle_ttl && !b.failed_simulation()
        });

        // Greedy knapsack by value density (tip per compute unit).
        buf.sort_by(|a, b| {
            let da = a.tip_lamports as f64 / a.cu().max(1) as f64;
            let db = b.tip_lamports as f64 / b.cu().max(1) as f64;
            db.partial_cmp(&da).unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut winners = Vec::new();
        let mut used_cu = 0u64;
        let mut total_tip = 0u64;
        for pb in buf.drain(..) {
            let cu = pb.cu();
            let uuid = pb.bundle.uuid.clone();
            if used_cu.saturating_add(cu) <= self.cu_budget {
                used_cu += cu;
                total_tip = total_tip.saturating_add(pb.tip_lamports);
                if let Some(r) = &self.results {
                    // Reported as accepted/forwarded by the engine. (slot and
                    // validator identity are populated at forward time later.)
                    r.publish_accepted(&uuid, 0, String::new());
                }
                winners.push(pb.bundle);
            } else if let Some(r) = &self.results {
                // Bid wasn't high enough to fit the winning set this round.
                r.publish_lost_auction(&uuid, String::new(), pb.tip_lamports);
            }
        }

        jito_metrics::add_bundles_won(winners.len() as u64);
        jito_metrics::add_bundles_dropped((entered - winners.len()) as u64);
        if !winners.is_empty() {
            info!(
                "auction: {} winners, {} lamports tip, {} CU used",
                winners.len(),
                total_tip,
                used_cu
            );
        }
        winners
    }
}

/// Decode a protobuf packet's bytes into a VersionedTransaction.
fn decode_tx(packet: &Packet) -> Option<VersionedTransaction> {
    deserialize::<VersionedTransaction>(&packet.data).ok()
}
