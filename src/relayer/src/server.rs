//! Implements the `BlockEngineRelayer` gRPC service — the half of the protocol
//! that the reference `block_engine_simple` never built.
//!
//! Data flow this wires up:
//!
//! ```text
//!   relayer ──StartExpiringPacketStream──► [this service] ──packet_sender──► validator forwarder ──► validator
//!   relayer ◄─SubscribeAccountsOfInterest─ [this service]   (engine tells relayer which state it cares about)
//! ```
//!
//! The relayer opens a *bidirectional* stream and pushes `PacketBatchUpdate`
//! messages (either real packet batches or heartbeats). We pull the batches out
//! and shove them into the same `packet_sender` channel the validator service
//! drains, so packets the relayer collects finally reach subscribed validators.
//!
//! Skeleton scope / TODO before production:
//!   * Accounts/Programs of Interest are hard-coded to "*" (everything). A real
//!     engine derives these from the bundles searchers submit, so the relayer
//!     only forwards transactions that write-lock contended state.
//!   * No auth interceptor yet (see the auth crate).
//!   * Expiry (`expiry_ms`) is ignored — a real engine must drop/forward a batch
//!     before the relayer's ~200ms hold elapses.

use std::time::Duration;

use jito_protos::block_engine::{
    block_engine_relayer_server::BlockEngineRelayer, packet_batch_update::Msg,
    AccountsOfInterestRequest, AccountsOfInterestUpdate, PacketBatchUpdate,
    ProgramsOfInterestRequest, ProgramsOfInterestUpdate, StartExpiringPacketStreamResponse,
};
use jito_protos::packet::PacketBatch;
use jito_protos::shared::Heartbeat;
use log::{info, warn};
use tokio::sync::mpsc::{channel, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// How often we re-advertise our accounts/programs of interest to the relayer,
/// and how often we heartbeat the packet stream back to it.
const UPDATE_INTERVAL: Duration = Duration::from_secs(5);

pub struct RelayerServerImpl {
    /// Sends packet batches received from the relayer into the validator
    /// forwarder. This is the `packet_sender` half that `main.rs` previously
    /// left unused (`_packet_sender`).
    packet_sender: Sender<PacketBatch>,
}

impl RelayerServerImpl {
    pub fn new(packet_sender: Sender<PacketBatch>) -> Self {
        Self { packet_sender }
    }
}

#[tonic::async_trait]
impl BlockEngineRelayer for RelayerServerImpl {
    type SubscribeAccountsOfInterestStream =
        ReceiverStream<Result<AccountsOfInterestUpdate, Status>>;

    /// Tell the relayer which accounts we want transactions for. Skeleton sends
    /// "*" (all accounts) on an interval so the relayer forwards everything.
    async fn subscribe_accounts_of_interest(
        &self,
        _request: Request<AccountsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeAccountsOfInterestStream>, Status> {
        info!("relayer subscribed to accounts of interest");
        let (sender, receiver) = channel(16);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(UPDATE_INTERVAL);
            loop {
                interval.tick().await;
                let update = AccountsOfInterestUpdate {
                    accounts: vec!["*".to_string()],
                };
                if sender.send(Ok(update)).await.is_err() {
                    warn!("relayer AOI stream closed");
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type SubscribeProgramsOfInterestStream =
        ReceiverStream<Result<ProgramsOfInterestUpdate, Status>>;

    async fn subscribe_programs_of_interest(
        &self,
        _request: Request<ProgramsOfInterestRequest>,
    ) -> Result<Response<Self::SubscribeProgramsOfInterestStream>, Status> {
        info!("relayer subscribed to programs of interest");
        let (sender, receiver) = channel(16);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(UPDATE_INTERVAL);
            loop {
                interval.tick().await;
                let update = ProgramsOfInterestUpdate {
                    programs: vec!["*".to_string()],
                };
                if sender.send(Ok(update)).await.is_err() {
                    warn!("relayer POI stream closed");
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    type StartExpiringPacketStreamStream =
        ReceiverStream<Result<StartExpiringPacketStreamResponse, Status>>;

    /// Bidirectional: the relayer streams us packets + heartbeats; we stream
    /// heartbeats back (the comment in the proto explains the Envoy workaround
    /// that forces this to be bidirectional).
    async fn start_expiring_packet_stream(
        &self,
        request: Request<Streaming<PacketBatchUpdate>>,
    ) -> Result<Response<Self::StartExpiringPacketStreamStream>, Status> {
        info!("relayer opened expiring packet stream");
        let mut inbound = request.into_inner();
        let packet_sender = self.packet_sender.clone();

        // Outbound heartbeats back to the relayer.
        let (hb_sender, hb_receiver) = channel(16);

        // Inbound: pull packet batches off the relayer's stream into the
        // validator forwarder.
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(update)) => match update.msg {
                        Some(Msg::Batches(expiring)) => {
                            if let Some(batch) = expiring.batch {
                                // TODO: honor `expiring.expiry_ms` — drop the
                                // batch if we can't act before the relayer
                                // forwards it directly to the validator.
                                if packet_sender.send(batch).await.is_err() {
                                    warn!("validator forwarder gone, ending relayer stream");
                                    break;
                                }
                            }
                        }
                        Some(Msg::Heartbeat(hb)) => {
                            // Clock-sync signal from the relayer; a real engine
                            // tracks drift here. Skeleton just logs at trace.
                            log::trace!("relayer heartbeat count={}", hb.count);
                        }
                        None => {}
                    },
                    Ok(None) => {
                        info!("relayer closed expiring packet stream");
                        break;
                    }
                    Err(e) => {
                        warn!("error on relayer packet stream: {e}");
                        break;
                    }
                }
            }
        });

        // Outbound: heartbeat the relayer on an interval.
        tokio::spawn(async move {
            let mut count: u64 = 0;
            let mut interval = tokio::time::interval(UPDATE_INTERVAL);
            loop {
                interval.tick().await;
                count += 1;
                let resp = StartExpiringPacketStreamResponse {
                    heartbeat: Some(Heartbeat { count }),
                };
                if hb_sender.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(hb_receiver)))
    }
}
