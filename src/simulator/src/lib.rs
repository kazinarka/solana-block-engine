//! RPC-backed bundle simulation.
//!
//! Rather than maintaining bank state locally, we delegate execution to a Solana
//! RPC node (ideally your jito-solana validator). Two modes:
//!
//! * **per-tx** (default, portable): each transaction is simulated with
//!   `simulateTransaction` and `replace_recent_blockhash`; CU is summed. This
//!   does NOT capture state changes between transactions in the same bundle.
//! * **atomic** (`--sim-atomic`, jito-solana only): the whole bundle is
//!   simulated with jito-solana's custom `simulateBundle` JSON-RPC, which runs
//!   the transactions sequentially against shared state — the accurate model for
//!   bundles whose later txs depend on earlier ones.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
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
    http: reqwest::Client,
    rpc_url: String,
    /// Use jito-solana's atomic `simulateBundle` instead of per-tx simulation.
    atomic: bool,
}

impl RpcSimulator {
    pub fn new(rpc_url: String, atomic: bool) -> Self {
        Self {
            rpc: RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::processed()),
            http: reqwest::Client::new(),
            rpc_url,
            atomic,
        }
    }

    pub async fn simulate_bundle(&self, bundle: &BundleUuid) -> SimOutcome {
        if self.atomic {
            self.simulate_atomic(bundle).await
        } else {
            self.simulate_per_tx(bundle).await
        }
    }

    fn packets<'a>(bundle: &'a BundleUuid) -> &'a [Packet] {
        bundle
            .bundle
            .as_ref()
            .map(|b| b.packets.as_slice())
            .unwrap_or(&[])
    }

    /// Per-transaction simulation. Fails the bundle on the first tx error.
    async fn simulate_per_tx(&self, bundle: &BundleUuid) -> SimOutcome {
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        };

        let mut total_cu = 0u64;
        for packet in Self::packets(bundle) {
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

    /// Atomic simulation via jito-solana's `simulateBundle` JSON-RPC. The
    /// packet bytes are already serialized transactions, so we base64-encode
    /// them directly as `encodedTransactions`.
    async fn simulate_atomic(&self, bundle: &BundleUuid) -> SimOutcome {
        let encoded: Vec<String> = Self::packets(bundle)
            .iter()
            .map(|p| STANDARD.encode(&p.data))
            .collect();
        if encoded.is_empty() {
            return SimOutcome { ok: false, units_consumed: 0 };
        }

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "simulateBundle",
            "params": [
                { "encodedTransactions": encoded },
                { "skipSigVerify": true, "replaceRecentBlockhash": true }
            ]
        });

        let resp: serde_json::Value = match self
            .http
            .post(&self.rpc_url)
            .json(&request)
            .send()
            .await
            .and_then(|r| r.error_for_status())
        {
            Ok(r) => match r.json().await {
                Ok(v) => v,
                Err(e) => {
                    warn!("simulator: bad simulateBundle response for {}: {e}", bundle.uuid);
                    return SimOutcome { ok: false, units_consumed: 0 };
                }
            },
            Err(e) => {
                warn!("simulator: simulateBundle RPC error for {}: {e}", bundle.uuid);
                return SimOutcome { ok: false, units_consumed: 0 };
            }
        };

        if let Some(err) = resp.get("error") {
            debug!("simulator: simulateBundle returned error for {}: {err}", bundle.uuid);
            return SimOutcome { ok: false, units_consumed: 0 };
        }

        let value = &resp["result"]["value"];
        // `summary` is the string "succeeded" on success, otherwise an object
        // describing the failing transaction.
        let ok = value.get("summary") == Some(&serde_json::json!("succeeded"));
        let total_cu = value
            .get("transactionResults")
            .and_then(|r| r.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t.get("unitsConsumed").and_then(|u| u.as_u64()))
                    .sum()
            })
            .unwrap_or(0);

        SimOutcome { ok, units_consumed: total_cu }
    }
}
