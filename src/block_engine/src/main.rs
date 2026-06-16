use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use jito_auth::interceptor::AuthInterceptor;
use jito_auth::server::{random_secret, spawn_challenge_pruner, AuthServiceImpl};
use jito_auth::token::AuthState;
use jito_protos::auth::auth_service_server::AuthServiceServer;
use jito_protos::block_engine::block_engine_relayer_server::BlockEngineRelayerServer;
use jito_protos::block_engine::block_engine_validator_server::BlockEngineValidatorServer;
use jito_protos::searcher::searcher_service_server::SearcherServiceServer;
use jito_leader_tracker::LeaderTracker;
use jito_relayer_service::server::RelayerServerImpl;
use jito_searcher::server::SearcherServiceImpl;
use jito_validator::server::ValidatorServerImpl;
use log::{info, warn};
use tokio::runtime::Builder;
use tokio::sync::mpsc::channel;
use tonic::transport::Server;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Bind address for searcher service
    #[clap(long, env, default_value = "0.0.0.0:1234")]
    searcher_addr: SocketAddr,

    /// Bind address for validator service
    #[clap(long, env, default_value = "0.0.0.0:1003")]
    validator_addr: SocketAddr,

    /// Bind address for relayer service
    #[clap(long, env, default_value = "0.0.0.0:1004")]
    relayer_addr: SocketAddr,

    /// Bind address for auth service
    #[clap(long, env, default_value = "0.0.0.0:1005")]
    auth_addr: SocketAddr,

    /// HS256 secret used to sign/verify access tokens. If unset, a random
    /// secret is generated at startup (tokens won't survive a restart).
    #[clap(long, env = "AUTH_JWT_SECRET")]
    auth_jwt_secret: Option<String>,

    /// Comma-separated base58 pubkeys allowed to authenticate. If empty, ANY
    /// pubkey may connect (logged as a warning — set this in production).
    #[clap(long, env = "ALLOWED_PUBKEYS", value_delimiter = ',')]
    allowed_pubkeys: Vec<String>,

    /// Solana RPC url used to track the leader schedule. If unset, packets and
    /// bundles are forwarded to ALL connected validators (fine for local
    /// testing; set it in production for leader-targeted routing).
    #[clap(long, env = "LEADER_RPC_URL")]
    leader_rpc_url: Option<String>,

    /// How many slots ahead of the current slot a validator counts as the
    /// upcoming leader (so traffic arrives before its slot begins).
    #[clap(long, env, default_value_t = 2)]
    leader_lookahead_slots: u64,
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    // Shared auth state: the AuthService issues tokens against it, and the
    // interceptor on each protected service validates tokens against it.
    let jwt_secret = args
        .auth_jwt_secret
        .map(|s| s.into_bytes())
        .unwrap_or_else(|| {
            warn!("AUTH_JWT_SECRET not set; generating an ephemeral secret (tokens reset on restart)");
            random_secret()
        });
    let allowed_pubkeys = if args.allowed_pubkeys.is_empty() {
        warn!("ALLOWED_PUBKEYS is empty; ANY pubkey may authenticate — set this in production");
        None
    } else {
        Some(args.allowed_pubkeys.into_iter().collect::<HashSet<_>>())
    };
    let auth_state = Arc::new(AuthState::new(jwt_secret, allowed_pubkeys));

    // The packet path: the relayer service pushes batches into `packet_sender`,
    // and the validator forwarder drains `packet_receiver`.
    let (packet_sender, packet_receiver) = channel(100);
    let (bundle_sender, bundle_receiver) = channel(100);

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        spawn_challenge_pruner(auth_state.clone());

        // Leader-schedule tracker for routing. None => forward to all.
        let leader_tracker = match args.leader_rpc_url {
            Some(url) => {
                info!("leader-targeted routing enabled via RPC {url}");
                Some(LeaderTracker::start(url, args.leader_lookahead_slots))
            }
            None => {
                warn!("LEADER_RPC_URL not set; forwarding to ALL validators (no leader routing)");
                None
            }
        };

        // start searcher server (token-protected)
        {
            let interceptor = AuthInterceptor::new(auth_state.clone());
            tokio::spawn(async move {
                let searcher_service_impl = SearcherServiceImpl::new(bundle_sender);
                let searcher_svc =
                    SearcherServiceServer::with_interceptor(searcher_service_impl, interceptor);
                info!("starting searcher server at {}", args.searcher_addr);
                Server::builder()
                    .add_service(searcher_svc)
                    .serve(args.searcher_addr)
                    .await
                    .expect("searcher server starts");
            });
        }

        // start relayer server (token-protected) — ingests packets from the
        // relayer into the validator forwarder
        {
            let interceptor = AuthInterceptor::new(auth_state.clone());
            tokio::spawn(async move {
                let relayer_service_impl = RelayerServerImpl::new(packet_sender);
                let relayer_svc =
                    BlockEngineRelayerServer::with_interceptor(relayer_service_impl, interceptor);
                info!("starting relayer server at {}", args.relayer_addr);
                Server::builder()
                    .add_service(relayer_svc)
                    .serve(args.relayer_addr)
                    .await
                    .expect("relayer server starts");
            });
        }

        // start auth server (NOT token-protected — clients call it to get tokens)
        {
            let auth_state = auth_state.clone();
            tokio::spawn(async move {
                let auth_service_impl = AuthServiceImpl::new(auth_state);
                let auth_svc = AuthServiceServer::new(auth_service_impl);
                info!("starting auth server at {}", args.auth_addr);
                Server::builder()
                    .add_service(auth_svc)
                    .serve(args.auth_addr)
                    .await
                    .expect("auth server starts");
            });
        }

        // start validator server (token-protected)
        let interceptor = AuthInterceptor::new(auth_state.clone());
        let validator_impl =
            ValidatorServerImpl::new(bundle_receiver, packet_receiver, leader_tracker);
        let validator_svc =
            BlockEngineValidatorServer::with_interceptor(validator_impl, interceptor);
        info!("starting validator server at {}", args.validator_addr);
        Server::builder()
            .add_service(validator_svc)
            .serve(args.validator_addr)
            .await
            .expect("validator server starts");
    });
}
