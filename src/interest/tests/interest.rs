//! Tests for accounts/programs-of-interest derivation.

use std::time::Duration;

use bincode::serialize;
use jito_interest::InterestRegistry;
use jito_protos::bundle::{Bundle, BundleUuid};
use jito_protos::packet::{Meta, Packet};
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::VersionedTransaction;
use solana_system_transaction as system_transaction;

/// The System Program id in base58 (all-1s).
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

fn transfer_bundle(from: &Keypair, to: &Pubkey, lamports: u64) -> BundleUuid {
    let tx = VersionedTransaction::from(system_transaction::transfer(
        from,
        to,
        lamports,
        Hash::default(),
    ));
    let data = serialize(&tx).unwrap();
    let size = data.len() as u64;
    let packet = Packet {
        data,
        meta: Some(Meta {
            size,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    };
    BundleUuid {
        uuid: "b".to_string(),
        bundle: Some(Bundle {
            header: None,
            packets: vec![packet],
        }),
    }
}

#[test]
fn captures_writable_accounts_and_programs() {
    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let reg = InterestRegistry::new(Duration::from_secs(5));
    reg.observe_bundle(&transfer_bundle(&from, &to, 1_000));

    let accounts = reg.accounts_of_interest();
    // Both the funding (signer) and destination accounts are writable.
    assert!(accounts.contains(&from.pubkey().to_string()), "payer is writable");
    assert!(accounts.contains(&to.to_string()), "destination is writable");
    // The System Program is read-only here, so it's NOT an account of interest...
    assert!(!accounts.contains(&SYSTEM_PROGRAM.to_string()));

    // ...but it IS a program of interest.
    assert!(reg
        .programs_of_interest()
        .contains(&SYSTEM_PROGRAM.to_string()));
}

#[test]
fn entries_expire_after_ttl() {
    let from = Keypair::new();
    let to = Pubkey::new_unique();
    let reg = InterestRegistry::new(Duration::from_millis(1));
    reg.observe_bundle(&transfer_bundle(&from, &to, 1_000));

    std::thread::sleep(Duration::from_millis(10));

    assert!(reg.accounts_of_interest().is_empty(), "AOI should expire");
    assert!(reg.programs_of_interest().is_empty(), "POI should expire");
}

#[test]
fn empty_registry_has_no_interest() {
    let reg = InterestRegistry::new(Duration::from_secs(5));
    assert!(reg.accounts_of_interest().is_empty());
    assert!(reg.programs_of_interest().is_empty());
}
