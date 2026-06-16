//! Tests for tip extraction and winner selection.

use std::collections::HashSet;
use std::time::Duration;

use bincode::serialize;
use jito_auction::{Auction, SimOutcome, EST_CU_PER_TX};
use jito_protos::bundle::{Bundle, BundleUuid};
use jito_protos::packet::{Meta, Packet};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::system_transaction;
use solana_sdk::transaction::VersionedTransaction;

/// A packet carrying a single SystemProgram transfer of `lamports` to `to`.
fn transfer_packet(from: &Keypair, to: &Pubkey, lamports: u64) -> Packet {
    let tx = VersionedTransaction::from(system_transaction::transfer(
        from,
        to,
        lamports,
        Hash::default(),
    ));
    let data = serialize(&tx).unwrap();
    let size = data.len() as u64;
    Packet {
        data,
        meta: Some(Meta {
            size,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    }
}

fn bundle(uuid: &str, packets: Vec<Packet>) -> BundleUuid {
    BundleUuid {
        uuid: uuid.to_string(),
        bundle: Some(Bundle {
            header: None,
            packets,
        }),
    }
}

fn uuids(winners: &[BundleUuid]) -> Vec<String> {
    winners.iter().map(|w| w.uuid.clone()).collect()
}

#[test]
fn selects_a_tipping_bundle() {
    let tip = Pubkey::new_unique();
    let auction = Auction::new(
        HashSet::from([tip]),
        48_000_000,
        Duration::from_secs(5),
    );
    let payer = Keypair::new();
    auction.submit(bundle("b1", vec![transfer_packet(&payer, &tip, 5_000)]));

    let winners = auction.run_auction();
    assert_eq!(uuids(&winners), vec!["b1"]);
}

#[test]
fn ranks_by_tip_descending() {
    let tip = Pubkey::new_unique();
    let auction = Auction::new(
        HashSet::from([tip]),
        48_000_000,
        Duration::from_secs(5),
    );
    let payer = Keypair::new();
    auction.submit(bundle("low", vec![transfer_packet(&payer, &tip, 1_000)]));
    auction.submit(bundle("high", vec![transfer_packet(&payer, &tip, 9_000)]));

    // Equal CU (one tx each), so higher tip ranks first.
    assert_eq!(uuids(&auction.run_auction()), vec!["high", "low"]);
}

#[test]
fn respects_cu_budget() {
    let tip = Pubkey::new_unique();
    // Budget for exactly one transaction's worth of CU.
    let auction = Auction::new(
        HashSet::from([tip]),
        EST_CU_PER_TX,
        Duration::from_secs(5),
    );
    let payer = Keypair::new();
    auction.submit(bundle("a", vec![transfer_packet(&payer, &tip, 1_000)]));
    auction.submit(bundle("b", vec![transfer_packet(&payer, &tip, 9_000)]));

    let winners = auction.run_auction();
    assert_eq!(winners.len(), 1, "only one bundle fits the CU budget");
    assert_eq!(uuids(&winners), vec!["b"], "the higher-tip bundle wins the slot");
}

#[test]
fn only_counts_transfers_to_tip_accounts() {
    let tip = Pubkey::new_unique();
    let other = Pubkey::new_unique();
    let auction = Auction::new(
        HashSet::from([tip]),
        EST_CU_PER_TX, // room for one bundle
        Duration::from_secs(5),
    );
    let payer = Keypair::new();
    auction.submit(bundle("tipper", vec![transfer_packet(&payer, &tip, 1_000)]));
    // A much larger transfer, but to a non-tip account — scores 0 tip.
    auction.submit(bundle("nontipper", vec![transfer_packet(&payer, &other, 1_000_000)]));

    let winners = auction.run_auction();
    assert_eq!(
        uuids(&winners),
        vec!["tipper"],
        "1000 lamports to the tip account beats 1M to a non-tip account"
    );
}

#[test]
fn empty_auction_returns_no_winners() {
    let auction = Auction::new(HashSet::new(), 48_000_000, Duration::from_secs(5));
    assert!(auction.run_auction().is_empty());
}

#[test]
fn failed_simulation_drops_bundle() {
    let tip = Pubkey::new_unique();
    let auction = Auction::new(HashSet::from([tip]), 48_000_000, Duration::from_secs(5));
    let payer = Keypair::new();
    auction.submit(bundle("doomed", vec![transfer_packet(&payer, &tip, 5_000)]));

    auction.set_simulation("doomed", SimOutcome { ok: false, units_consumed: 1_000 });

    assert!(
        auction.run_auction().is_empty(),
        "a bundle that fails simulation must not win"
    );
}

#[test]
fn pending_for_simulation_tracks_unsimulated_bundles() {
    let tip = Pubkey::new_unique();
    let auction = Auction::new(HashSet::from([tip]), 48_000_000, Duration::from_secs(5));
    let payer = Keypair::new();
    auction.submit(bundle("x", vec![transfer_packet(&payer, &tip, 1)]));

    assert_eq!(auction.pending_for_simulation().len(), 1);
    auction.set_simulation("x", SimOutcome { ok: true, units_consumed: 1_000 });
    assert!(
        auction.pending_for_simulation().is_empty(),
        "simulated bundles are no longer pending"
    );
}

#[test]
fn real_cu_replaces_estimate_for_packing() {
    let tip = Pubkey::new_unique();
    // Budget fits only one *estimated* bundle (200k each), but two real ones at
    // 100k each.
    let auction = Auction::new(HashSet::from([tip]), 250_000, Duration::from_secs(5));
    let payer = Keypair::new();
    auction.submit(bundle("a", vec![transfer_packet(&payer, &tip, 1_000)]));
    auction.submit(bundle("b", vec![transfer_packet(&payer, &tip, 2_000)]));

    // Before simulation: estimate is EST_CU_PER_TX each, so only one fits.
    assert_eq!(EST_CU_PER_TX, 200_000);

    // Simulate both as cheap (100k each) → both now fit the 250k budget.
    auction.set_simulation("a", SimOutcome { ok: true, units_consumed: 100_000 });
    auction.set_simulation("b", SimOutcome { ok: true, units_consumed: 100_000 });

    let winners = auction.run_auction();
    assert_eq!(winners.len(), 2, "real CU lets both bundles fit");
}
