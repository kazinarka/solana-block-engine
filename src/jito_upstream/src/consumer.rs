use std::sync::Arc;
use std::time::Duration;

use jito_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use jito_protos::block_engine::{
    BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, GetBlockEngineEndpointRequest,
    GetBlockEngineEndpointResponse, SubscribeBundlesRequest, SubscribePacketsRequest,
};
use jito_protos::bundle::BundleUuid;
use jito_protos::packet::PacketBatch;
use log::{info, warn};
use solana_sdk::signature::Keypair;
use tokio::sync::{mpsc, watch};
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::auth::TokenManager;
use crate::tls::{connect, connect_tls};

const CHANNEL_CAPACITY: usize = 1000;
const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

type SessionResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub struct UpstreamConfig {
    pub url: String,
    pub tls: bool,
}

pub struct Upstream {
    pub bundles: mpsc::Receiver<BundleUuid>,
    pub packets: mpsc::Receiver<PacketBatch>,
    pub fee_info: watch::Receiver<Option<BlockBuilderFeeInfoResponse>>,
    pub endpoints: watch::Receiver<Option<GetBlockEngineEndpointResponse>>,
}

pub fn start(config: UpstreamConfig, keypair: Arc<Keypair>) -> Upstream {
    let (bundle_tx, bundle_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (packet_tx, packet_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (fee_tx, fee_rx) = watch::channel(None);
    let (ep_tx, ep_rx) = watch::channel(None);

    tokio::spawn(run(config, keypair, bundle_tx, packet_tx, fee_tx, ep_tx));

    Upstream {
        bundles: bundle_rx,
        packets: packet_rx,
        fee_info: fee_rx,
        endpoints: ep_rx,
    }
}

async fn run(
    config: UpstreamConfig,
    keypair: Arc<Keypair>,
    bundle_tx: mpsc::Sender<BundleUuid>,
    packet_tx: mpsc::Sender<PacketBatch>,
    fee_tx: watch::Sender<Option<BlockBuilderFeeInfoResponse>>,
    ep_tx: watch::Sender<Option<GetBlockEngineEndpointResponse>>,
) {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match session(&config, &keypair, &bundle_tx, &packet_tx, &fee_tx, &ep_tx).await {
            Ok(()) => {
                jito_metrics::inc_upstream_disconnects();
                info!("upstream session ended; reconnecting");
                backoff = INITIAL_BACKOFF;
            }
            Err(e) => {
                jito_metrics::inc_upstream_disconnects();
                warn!("upstream session error: {e}; reconnecting in {backoff:?}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn session(
    config: &UpstreamConfig,
    keypair: &Arc<Keypair>,
    bundle_tx: &mpsc::Sender<BundleUuid>,
    packet_tx: &mpsc::Sender<PacketBatch>,
    fee_tx: &watch::Sender<Option<BlockBuilderFeeInfoResponse>>,
    ep_tx: &watch::Sender<Option<GetBlockEngineEndpointResponse>>,
) -> SessionResult {
    let channel = if config.tls {
        connect_tls(config.url.clone()).await?
    } else {
        connect(config.url.clone()).await?
    };

    let token_manager = TokenManager::start(channel.clone(), keypair.clone()).await?;
    let auth = token_manager.clone();
    let mut client = BlockEngineValidatorClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            let bearer: MetadataValue<_> = auth
                .bearer()
                .parse()
                .map_err(|_| Status::internal("invalid bearer token"))?;
            req.metadata_mut().insert("authorization", bearer);
            Ok(req)
        },
    );

    if let Ok(resp) = client
        .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
        .await
    {
        let _ = fee_tx.send(Some(resp.into_inner()));
    }
    if let Ok(resp) = client
        .get_block_engine_endpoints(GetBlockEngineEndpointRequest {})
        .await
    {
        let _ = ep_tx.send(Some(resp.into_inner()));
    }

    let mut bundles = client
        .subscribe_bundles(SubscribeBundlesRequest {})
        .await?
        .into_inner();
    let mut packets = client
        .subscribe_packets(SubscribePacketsRequest {})
        .await?
        .into_inner();
    jito_metrics::inc_upstream_connects();
    info!("upstream subscribed to bundles and packets");

    loop {
        tokio::select! {
            message = bundles.message() => match message? {
                Some(resp) => {
                    for bundle in resp.bundles {
                        if bundle_tx.send(bundle).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                None => return Ok(()),
            },
            message = packets.message() => match message? {
                Some(resp) => {
                    if let Some(batch) = resp.batch {
                        if packet_tx.send(batch).await.is_err() {
                            return Ok(());
                        }
                    }
                }
                None => return Ok(()),
            },
        }
    }
}
