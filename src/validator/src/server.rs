use jito_auth::token::Claims;
use jito_leader_tracker::LeaderTracker;
use jito_protos::packet::PacketBatch;
use jito_protos::{
    block_engine::{
        block_engine_validator_server::BlockEngineValidator, BlockBuilderFeeInfoRequest,
        BlockBuilderFeeInfoResponse, GetBlockEngineEndpointRequest, GetBlockEngineEndpointResponse,
        SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
        SubscribePacketsResponse,
    },
    bundle::BundleUuid,
};
use log::{info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{Builder, JoinHandle};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// A subscribing validator stream, tagged with the validator's identity (its
/// base58 pubkey, taken from the auth token) so we can route by leader schedule.
struct Subscription<T> {
    identity: String,
    sender: Sender<Result<T, Status>>,
}

type PacketSubs = Arc<Mutex<HashMap<Uuid, Subscription<SubscribePacketsResponse>>>>;
type BundleSubs = Arc<Mutex<HashMap<Uuid, Subscription<SubscribeBundlesResponse>>>>;

pub struct ValidatorServerImpl {
    forwarder_thread: JoinHandle<()>,
    packet_subscriptions: PacketSubs,
    bundle_subscriptions: BundleSubs,
}

/// Should this subscriber receive traffic right now? Yes if no leader tracker is
/// configured, otherwise only if its identity is an upcoming leader.
fn should_forward(tracker: &Option<Arc<LeaderTracker>>, identity: &str) -> bool {
    match tracker {
        Some(t) => t.is_upcoming_leader(identity),
        None => true,
    }
}

/// Pull the authenticated identity (base58 pubkey) out of the request, as set by
/// the auth interceptor. Empty if missing (which, with auth enabled, shouldn't
/// happen — such a stream simply never matches a leader).
fn identity_of<T>(req: &Request<T>) -> String {
    req.extensions()
        .get::<Claims>()
        .map(|c| c.sub.clone())
        .unwrap_or_default()
}

impl ValidatorServerImpl {
    pub fn new(
        bundle_receiver: Receiver<BundleUuid>,
        packet_receiver: Receiver<PacketBatch>,
        leader_tracker: Option<Arc<LeaderTracker>>,
    ) -> Self {
        let packet_subscriptions = Arc::new(Mutex::new(HashMap::default()));
        let bundle_subscriptions = Arc::new(Mutex::new(HashMap::default()));
        let forwarder_thread = Self::start_forwarder_thread(
            bundle_receiver,
            packet_receiver,
            &packet_subscriptions,
            &bundle_subscriptions,
            leader_tracker,
        );
        Self {
            forwarder_thread,
            packet_subscriptions,
            bundle_subscriptions,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.forwarder_thread.join()
    }

    fn start_forwarder_thread(
        mut bundle_receiver: Receiver<BundleUuid>,
        mut packet_receiver: Receiver<PacketBatch>,
        packet_subscriptions: &PacketSubs,
        bundle_subscriptions: &BundleSubs,
        leader_tracker: Option<Arc<LeaderTracker>>,
    ) -> JoinHandle<()> {
        let packet_subscriptions = packet_subscriptions.clone();
        let bundle_subscriptions = bundle_subscriptions.clone();
        Builder::new()
            .name("forwarder_thread".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async move {
                    loop {
                        tokio::select! {
                            maybe_packet_batch = packet_receiver.recv() => {
                                if let Some(packet_batch) = maybe_packet_batch {
                                    let failed_sends = Self::forward_packets(packet_batch, &packet_subscriptions, &leader_tracker).await;
                                    for uuid in failed_sends {
                                        info!("removing packet_subscriptions uuid: {:?}", uuid);
                                        packet_subscriptions.lock().unwrap().remove(&uuid);
                                    }
                                } else {
                                    warn!("packet_receiver disconnected, exiting");
                                    break;
                                }
                            }
                            maybe_bundle = bundle_receiver.recv() => {
                                if let Some(bundle) = maybe_bundle {
                                    let failed_sends = Self::forward_bundle(bundle, &bundle_subscriptions, &leader_tracker).await;
                                    for uuid in failed_sends {
                                        info!("removing bundle_subscriptions uuid: {:?}", uuid);
                                        bundle_subscriptions.lock().unwrap().remove(&uuid);
                                    }
                                } else {
                                    warn!("bundle_receiver disconnected, exiting");
                                    break;
                                }
                            }
                        }
                    }
                })
            })
            .unwrap()
    }

    async fn forward_packets(
        packet_batch: PacketBatch,
        packet_subscriptions: &PacketSubs,
        leader_tracker: &Option<Arc<LeaderTracker>>,
    ) -> Vec<Uuid> {
        let mut failed_sends = Vec::new();
        let subs = packet_subscriptions.lock().unwrap();
        for (uuid, sub) in subs.iter() {
            if !should_forward(leader_tracker, &sub.identity) {
                continue;
            }
            match sub.sender.try_send(Ok(SubscribePacketsResponse {
                header: None,
                batch: Some(packet_batch.clone()),
            })) {
                Ok(_) => {}
                Err(TrySendError::Closed(_)) => {
                    failed_sends.push(*uuid);
                }
                Err(TrySendError::Full(_)) => {
                    warn!("packet channel full uuid: {:?}", uuid);
                }
            }
        }
        failed_sends
    }

    async fn forward_bundle(
        bundle: BundleUuid,
        bundle_subscriptions: &BundleSubs,
        leader_tracker: &Option<Arc<LeaderTracker>>,
    ) -> Vec<Uuid> {
        let mut failed_sends = Vec::new();
        let subs = bundle_subscriptions.lock().unwrap();
        for (uuid, sub) in subs.iter() {
            if !should_forward(leader_tracker, &sub.identity) {
                continue;
            }
            match sub.sender.try_send(Ok(SubscribeBundlesResponse {
                bundles: vec![bundle.clone()],
            })) {
                Ok(_) => {
                    info!("bundle forwarded to validator uuid: {:?}", uuid);
                }
                Err(TrySendError::Closed(_)) => {
                    warn!("bundle channel closed validator uuid: {:?}", uuid);
                    failed_sends.push(*uuid);
                }
                Err(TrySendError::Full(_)) => {
                    warn!("bundle channel full validator uuid: {:?}", uuid);
                }
            }
        }
        failed_sends
    }
}

#[tonic::async_trait]
impl BlockEngineValidator for ValidatorServerImpl {
    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let identity = identity_of(&request);
        let (sender, receiver) = channel(1000);
        let uuid = Uuid::new_v4();

        info!("adding packet subscription uuid={uuid:?} identity={identity}");
        self.packet_subscriptions
            .lock()
            .unwrap()
            .insert(uuid, Subscription { identity, sender });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type SubscribeBundlesStream = ReceiverStream<Result<SubscribeBundlesResponse, Status>>;

    async fn subscribe_bundles(
        &self,
        request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        let identity = identity_of(&request);
        let (sender, receiver) = channel(1000);
        let uuid = Uuid::new_v4();

        info!("adding bundle subscription uuid={uuid:?} identity={identity}");
        self.bundle_subscriptions
            .lock()
            .unwrap()
            .insert(uuid, Subscription { identity, sender });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        // TODO: return the block builder's real tip-distribution pubkey. The
        // all-1s base58 string is `Pubkey::default()` (32 zero bytes) — a
        // placeholder so this crate needn't depend on solana-sdk.
        let response = BlockBuilderFeeInfoResponse {
            pubkey: "11111111111111111111111111111111".to_string(),
            commission: 5,
        };

        info!("get_block_builder_fee_info response: {:?}", response);

        Ok(Response::new(response))
    }

    async fn get_block_engine_endpoints(
        &self,
        _request: Request<GetBlockEngineEndpointRequest>,
    ) -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        // Endpoint discovery: a multi-region deployment would advertise its
        // global + regioned URLs here. Single-node skeleton returns none.
        Ok(Response::new(GetBlockEngineEndpointResponse {
            global_endpoint: None,
            regioned_endpoints: vec![],
        }))
    }
}
