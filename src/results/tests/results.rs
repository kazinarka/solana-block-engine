//! Tests for per-searcher bundle-result routing.

use jito_protos::bundle::{bundle_result::Result as ResultKind, BundleResult, DroppedReason};
use jito_results::BundleResults;
use tokio::sync::mpsc::channel;
use tonic::Status;

type Item = Result<BundleResult, Status>;

#[test]
fn delivers_result_to_owning_searcher() {
    let hub = BundleResults::new();
    let (tx, mut rx) = channel::<Item>(4);
    hub.add_subscriber("owner1", tx);
    hub.register("uuidA", "owner1");

    hub.publish_accepted("uuidA", 5, "val1".to_string());

    let item = rx.try_recv().expect("result delivered").expect("ok");
    assert_eq!(item.bundle_id, "uuidA");
    assert!(matches!(item.result, Some(ResultKind::Accepted(_))));
}

#[test]
fn does_not_leak_to_other_searchers() {
    let hub = BundleResults::new();
    let (tx1, mut rx1) = channel::<Item>(4);
    let (tx2, mut rx2) = channel::<Item>(4);
    hub.add_subscriber("owner1", tx1);
    hub.add_subscriber("owner2", tx2);
    hub.register("u", "owner1");

    hub.publish_lost_auction("u", String::new(), 100);

    assert!(rx1.try_recv().is_ok(), "owner1 receives its result");
    assert!(rx2.try_recv().is_err(), "owner2 receives nothing");
}

#[test]
fn unregistered_bundle_is_dropped() {
    let hub = BundleResults::new();
    let (tx, mut rx) = channel::<Item>(4);
    hub.add_subscriber("owner1", tx);

    // "ghost" was never registered to any owner.
    hub.publish_accepted("ghost", 0, String::new());
    assert!(rx.try_recv().is_err());
}

#[test]
fn result_is_terminal_per_bundle() {
    let hub = BundleResults::new();
    let (tx, mut rx) = channel::<Item>(4);
    hub.add_subscriber("owner1", tx);
    hub.register("u", "owner1");

    hub.publish_sim_failure("u", "boom".to_string());
    assert!(rx.try_recv().is_ok(), "first result delivered");

    // The uuid->owner mapping is consumed, so a second publish delivers nothing.
    hub.publish_accepted("u", 1, "v".to_string());
    assert!(rx.try_recv().is_err(), "no second result for the same bundle");
}

#[test]
fn processed_is_non_terminal_then_finalized_is_terminal() {
    let hub = BundleResults::new();
    let (tx, mut rx) = channel::<Item>(8);
    hub.add_subscriber("owner1", tx);
    hub.register("u", "owner1");

    hub.publish_accepted("u", 10, "val".to_string());
    hub.publish_processed("u", 11, "val".to_string());
    hub.publish_finalized("u");

    let first = rx.try_recv().unwrap().unwrap();
    assert!(matches!(first.result, Some(ResultKind::Accepted(_))));
    let second = rx.try_recv().unwrap().unwrap();
    assert!(matches!(second.result, Some(ResultKind::Processed(_))));
    let third = rx.try_recv().unwrap().unwrap();
    assert!(matches!(third.result, Some(ResultKind::Finalized(_))));

    hub.publish_processed("u", 12, "val".to_string());
    assert!(rx.try_recv().is_err(), "nothing delivered after finalized");
}

#[test]
fn dropped_is_terminal() {
    let hub = BundleResults::new();
    let (tx, mut rx) = channel::<Item>(4);
    hub.add_subscriber("owner1", tx);
    hub.register("u", "owner1");

    hub.publish_dropped("u", DroppedReason::BlockhashExpired);
    let result = rx.try_recv().unwrap().unwrap();
    assert!(matches!(result.result, Some(ResultKind::Dropped(_))));

    hub.publish_finalized("u");
    assert!(rx.try_recv().is_err(), "nothing delivered after dropped");
}
