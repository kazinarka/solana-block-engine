use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
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
use jito_protos::bundle::{Bundle, BundleUuid};
use log::info;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, default_value = "0.0.0.0:1010")]
    addr: SocketAddr,

    #[clap(long, default_value_t = 1000)]
    bundle_interval_ms: u64,
}

struct MockJito {
    bundle_interval_ms: u64,
}

#[tonic::async_trait]
impl BlockEngineValidator for MockJito {
    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    async fn subscribe_packets(
        &self,
        _request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (tx, rx) = channel(8);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(86400)).await;
            drop(tx);
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeBundlesStream = ReceiverStream<Result<SubscribeBundlesResponse, Status>>;

    async fn subscribe_bundles(
        &self,
        _request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        let (tx, rx) = channel(64);
        let interval = self.bundle_interval_ms;
        tokio::spawn(async move {
            let counter = AtomicU64::new(0);
            loop {
                tokio::time::sleep(Duration::from_millis(interval)).await;
                let n = counter.fetch_add(1, Ordering::Relaxed);
                let response = SubscribeBundlesResponse {
                    bundles: vec![BundleUuid {
                        bundle: Some(Bundle {
                            header: None,
                            packets: vec![],
                        }),
                        uuid: format!("jito-{n}"),
                    }],
                };
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_block_builder_fee_info(
        &self,
        _request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: "MockJitoFeeAccount".to_string(),
            commission: 5,
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

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    let state = Arc::new(AuthState::new(b"mock-jito".to_vec(), None));

    info!("mock jito block engine listening at {}", args.addr);
    Server::builder()
        .add_service(AuthServiceServer::new(AuthServiceImpl::new(state)))
        .add_service(BlockEngineValidatorServer::new(MockJito {
            bundle_interval_ms: args.bundle_interval_ms,
        }))
        .serve(args.addr)
        .await
        .expect("mock jito server starts");
}
