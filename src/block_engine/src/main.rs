use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use std::time::Duration;

use clap::Parser;
use jito_auction::Auction;
use jito_auth::interceptor::AuthInterceptor;
use jito_auth::server::{random_secret, spawn_challenge_pruner, AuthServiceImpl};
use jito_auth::token::AuthState;
use jito_interest::InterestRegistry;
use jito_protos::auth::auth_service_server::AuthServiceServer;
use jito_protos::auth::Role;
use jito_protos::block_engine::block_engine_relayer_server::BlockEngineRelayerServer;
use jito_protos::block_engine::block_engine_validator_server::BlockEngineValidatorServer;
use jito_protos::searcher::searcher_service_server::SearcherServiceServer;
use jito_leader_tracker::LeaderTracker;
use jito_relayer_service::server::RelayerServerImpl;
use jito_simulator::RpcSimulator;
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

    /// Comma-separated base58 tip accounts. Bundles are scored by lamports
    /// transferred to these accounts. If empty, every bundle scores 0 tip.
    #[clap(long, env = "TIP_ACCOUNTS", value_delimiter = ',')]
    tip_accounts: Vec<String>,

    /// How often (ms) to run the auction and emit winners to the validator.
    #[clap(long, env, default_value_t = 200)]
    auction_interval_ms: u64,

    /// Per-block compute-unit budget the winning bundle set must fit within.
    #[clap(long, env, default_value_t = 48_000_000)]
    block_cu_limit: u64,

    /// Bundles older than this (ms) are dropped before each auction.
    #[clap(long, env, default_value_t = 200)]
    bundle_ttl_ms: u64,

    /// Solana RPC url used to simulate bundles (real CU + drop failing bundles).
    /// If unset, bundles are scored with the coarse CU estimate and never
    /// dropped for failing. Can be the same RPC as --leader-rpc-url.
    #[clap(long, env = "SIM_RPC_URL")]
    sim_rpc_url: Option<String>,

    /// Forward ALL packets from the relayer (advertise "*") instead of only the
    /// accounts/programs of interest derived from submitted bundles.
    #[clap(long, env = "FORWARD_ALL_PACKETS")]
    forward_all_packets: bool,

    /// How long (ms) a writable account / program stays "of interest" after the
    /// bundle that referenced it was submitted.
    #[clap(long, env, default_value_t = 2_000)]
    interest_ttl_ms: u64,

    /// Base58 pubkey that collects the block-builder fee (returned to validators
    /// via GetBlockBuilderFeeInfo). Defaults to the system pubkey placeholder.
    #[clap(long, env, default_value = "11111111111111111111111111111111")]
    block_builder_pubkey: String,

    /// Block-builder commission percent (0-100).
    #[clap(long, env, default_value_t = 5)]
    block_builder_commission: u64,
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

        // Registry of accounts/programs of interest derived from submitted
        // bundles; the relayer service streams these to the relayer.
        let interest = Arc::new(InterestRegistry::new(Duration::from_millis(
            args.interest_ttl_ms,
        )));
        {
            let interest = interest.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    interest.prune();
                }
            });
        }

        // The auction buffers bundles from searchers and, on each tick, emits the
        // winning set to the validator via `bundle_sender`.
        let auction = Arc::new(Auction::from_config(
            &args.tip_accounts,
            args.block_cu_limit,
            Duration::from_millis(args.bundle_ttl_ms),
        ));

        // Optional RPC simulator: real CU + drop failing bundles. None => use
        // the coarse CU estimate and never drop for failure.
        let simulator = match args.sim_rpc_url {
            Some(url) => {
                info!("bundle simulation enabled via RPC {url}");
                Some(Arc::new(RpcSimulator::new(url)))
            }
            None => {
                warn!("SIM_RPC_URL not set; using estimated CU (no bundle simulation)");
                None
            }
        };

        // Auction tick: simulate any new bundles, then run the auction and
        // forward winners.
        {
            let auction = auction.clone();
            let simulator = simulator.clone();
            let interval_ms = args.auction_interval_ms;
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
                loop {
                    interval.tick().await;
                    if let Some(sim) = &simulator {
                        for (uuid, bundle) in auction.pending_for_simulation() {
                            let outcome = sim.simulate_bundle(&bundle).await;
                            auction.set_simulation(&uuid, outcome);
                        }
                    }
                    for winner in auction.run_auction() {
                        if bundle_sender.send(winner).await.is_err() {
                            warn!("validator bundle channel closed; stopping auction tick");
                            return;
                        }
                    }
                }
            });
        }

        // start searcher server (token-protected)
        {
            let interceptor = AuthInterceptor::for_role(auth_state.clone(), Role::Searcher as i32);
            let auction = auction.clone();
            let interest = interest.clone();
            tokio::spawn(async move {
                let searcher_service_impl = SearcherServiceImpl::new(auction, interest);
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
            let interceptor = AuthInterceptor::for_role(auth_state.clone(), Role::Relayer as i32);
            let interest = interest.clone();
            let forward_all = args.forward_all_packets;
            tokio::spawn(async move {
                let relayer_service_impl =
                    RelayerServerImpl::new(packet_sender, interest, forward_all);
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
        let interceptor = AuthInterceptor::for_role(auth_state.clone(), Role::Validator as i32);
        let validator_impl = ValidatorServerImpl::new(
            bundle_receiver,
            packet_receiver,
            leader_tracker,
            args.block_builder_pubkey,
            args.block_builder_commission,
        );
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
