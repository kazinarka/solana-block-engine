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
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bincode::deserialize;
use jito_protos::bundle::BundleUuid;
use jito_protos::packet::Packet;
use log::{debug, info};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction::SystemInstruction;
use solana_sdk::system_program;
use solana_sdk::transaction::VersionedTransaction;

/// Rough per-transaction compute-unit estimate used until real simulation lands.
pub const EST_CU_PER_TX: u64 = 200_000;

/// A bundle waiting in the auction, scored at submission time.
struct PendingBundle {
    bundle: BundleUuid,
    tip_lamports: u64,
    est_cu: u64,
    received: Instant,
}

pub struct Auction {
    buffer: Mutex<Vec<PendingBundle>>,
    tip_accounts: HashSet<Pubkey>,
    /// Per-block compute-unit ceiling the winning set must fit within.
    cu_budget: u64,
    /// Bundles older than this are dropped before each auction.
    bundle_ttl: Duration,
}

impl Auction {
    pub fn new(tip_accounts: HashSet<Pubkey>, cu_budget: u64, bundle_ttl: Duration) -> Self {
        Self {
            buffer: Mutex::new(Vec::new()),
            tip_accounts,
            cu_budget,
            bundle_ttl,
        }
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
        self.buffer.lock().unwrap().push(PendingBundle {
            bundle,
            tip_lamports,
            est_cu,
            received: Instant::now(),
        });
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

        let now = Instant::now();
        buf.retain(|b| now.duration_since(b.received) < self.bundle_ttl);

        // Greedy knapsack by value density (tip per compute unit).
        buf.sort_by(|a, b| {
            let da = a.tip_lamports as f64 / a.est_cu.max(1) as f64;
            let db = b.tip_lamports as f64 / b.est_cu.max(1) as f64;
            db.partial_cmp(&da).unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut winners = Vec::new();
        let mut used_cu = 0u64;
        let mut total_tip = 0u64;
        for pb in buf.drain(..) {
            if used_cu.saturating_add(pb.est_cu) <= self.cu_budget {
                used_cu += pb.est_cu;
                total_tip = total_tip.saturating_add(pb.tip_lamports);
                winners.push(pb.bundle);
            }
        }

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
