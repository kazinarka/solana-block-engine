//! RPC-backed bundle simulation.
//!
//! Rather than maintaining bank state locally, we delegate execution to a Solana
//! RPC node (ideally your jito-solana validator). Each transaction in a bundle
//! is simulated with `replace_recent_blockhash` so a stale bundle blockhash
//! doesn't fail scoring; we sum the compute units consumed and treat the bundle
//! as failed if any transaction errors.
//!
//! Caveat: per-transaction `simulateTransaction` does not capture state changes
//! between transactions within the same bundle. The fully-accurate primitive is
//! jito-solana's `simulateBundle` RPC — a worthwhile upgrade for bundles whose
//! later transactions depend on earlier ones.

use bincode::deserialize;
use jito_auction::SimOutcome;
use jito_protos::bundle::BundleUuid;
use jito_protos::packet::Packet;
use log::{debug, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSimulateTransactionConfig;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::transaction::VersionedTransaction;

pub struct RpcSimulator {
    rpc: RpcClient,
}

impl RpcSimulator {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc: RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed()),
        }
    }

    /// Simulate every transaction in the bundle, summing CU. Returns `ok=false`
    /// as soon as any transaction errors or can't be decoded/simulated.
    pub async fn simulate_bundle(&self, bundle: &BundleUuid) -> SimOutcome {
        let packets: &[Packet] = bundle
            .bundle
            .as_ref()
            .map(|b| b.packets.as_slice())
            .unwrap_or(&[]);

        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        };

        let mut total_cu = 0u64;
        for packet in packets {
            let tx: VersionedTransaction = match deserialize(&packet.data) {
                Ok(tx) => tx,
                Err(e) => {
                    warn!("simulator: undecodable tx in bundle {}: {e}", bundle.uuid);
                    return SimOutcome { ok: false, units_consumed: total_cu };
                }
            };

            match self
                .rpc
                .simulate_transaction_with_config(&tx, config.clone())
                .await
            {
                Ok(resp) => {
                    if let Some(err) = resp.value.err {
                        debug!("simulator: bundle {} tx failed: {err:?}", bundle.uuid);
                        return SimOutcome { ok: false, units_consumed: total_cu };
                    }
                    total_cu = total_cu.saturating_add(resp.value.units_consumed.unwrap_or(0));
                }
                Err(e) => {
                    warn!("simulator: RPC error simulating bundle {}: {e}", bundle.uuid);
                    return SimOutcome { ok: false, units_consumed: total_cu };
                }
            }
        }

        SimOutcome { ok: true, units_consumed: total_cu }
    }
}
