use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jito_auth::server::AuthServiceImpl;
use jito_auth::token::AuthState;
use jito_protos::auth::auth_service_server::AuthServiceServer;
use jito_protos::block_engine::block_engine_validator_server::{
    BlockEngineValidator, BlockEngineValidatorServer,
};
use jito_protos::block_engine::{
    BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, GetBlockEngineEndpointRequest,
    GetBlockEngineEndpointResponse, SubscribeBundlesRequest, SubscribeBundlesResponse,
    SubscribePacketsRequest, SubscribePacketsResponse,
};
use jito_protos::bundle::BundleUuid;
use jito_protos::packet::PacketBatch;
use jito_upstream::consumer::{start, UpstreamConfig};
use solana_sdk::signature::{Keypair, Signer};
use tokio::net::TcpListener;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

struct MockBlockEngine;

#[tonic::async_trait]
impl BlockEngineValidator for MockBlockEngine {
    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (tx, rx) = channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(SubscribePacketsResponse {
                    header: None,
                    batch: Some(PacketBatch { packets: vec![] }),
                }))
                .await;
            tokio::time::sleep(Duration::from_secs(3600)).await;
            drop(tx);
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeBundlesStream = ReceiverStream<Result<SubscribeBundlesResponse, Status>>;

    async fn subscribe_bundles(
        &self,
        _request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        let (tx, rx) = channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(SubscribeBundlesResponse {
                    bundles: vec![BundleUuid {
                        bundle: None,
                        uuid: "mock".to_string(),
                    }],
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: "MockFeeAccount".to_string(),
            commission: 7,
        }))
    }

    async fn get_block_engine_endpoints(
        &self,
        _request: Request<GetBlockEngineEndpointRequest>,
    ) -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        Ok(Response::new(GetBlockEngineEndpointResponse {
            global_endpoint: None,
            regioned_endpoints: vec![],
        }))
    }
}

#[tokio::test]
async fn consumes_bundles_relays_fee_info_and_reconnects() {
    let keypair = Arc::new(Keypair::new());
    let mut allowed = HashSet::new();
    allowed.insert(keypair.pubkey().to_string());
    let state = Arc::new(AuthState::new(b"test-secret".to_vec(), Some(allowed)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AuthServiceServer::new(AuthServiceImpl::new(state)))
            .add_service(BlockEngineValidatorServer::new(MockBlockEngine))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut upstream = start(
        UpstreamConfig {
            url: format!("http://{addr}"),
            tls: false,
        },
        keypair,
    );

    let mut received = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), upstream.bundles.recv()).await {
            Ok(Some(_)) => {
                received += 1;
                if received >= 2 {
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(
        received >= 2,
        "reconnect should deliver multiple bundles, got {received}"
    );

    let fee = upstream.fee_info.borrow().clone();
    assert_eq!(fee.unwrap().commission, 7);
}
