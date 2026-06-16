use std::{
    process::exit,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use bincode::serialize;
use clap::Parser;
use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::{GenerateAuthChallengeRequest, GenerateAuthTokensRequest};
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_protos::{
    bundle::Bundle,
    packet::{Meta, Packet},
    searcher::SendBundleRequest,
    shared::Header,
};
use log::{error, info};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signer},
    transaction::VersionedTransaction,
};
use solana_system_transaction as system_transaction;
use tokio::runtime::Builder;
use tokio::time::sleep;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// RPC url to request airdrop from
    #[clap(short, long, env, default_value_t = String::from("http://localhost:8899"))]
    rpc_url: String,

    /// URL for searcher service
    #[clap(short, long, env, default_value_t = String::from("http://localhost:1234"))]
    searcher_service_url: String,

    /// URL for the auth service
    #[clap(short, long, env, default_value_t = String::from("http://localhost:1005"))]
    auth_service_url: String,

    /// Path to the keypair used to authenticate and sign transactions.
    /// Its pubkey must be in the block engine's allowlist (--allowed-pubkeys).
    #[clap(short, long, env, default_value_t = String::from("./keypair.json"))]
    keypair_path: String,
}

/// Serialize a VersionedTransaction into a protobuf packet.
fn proto_packet_from_versioned_tx(tx: &VersionedTransaction) -> Packet {
    let data = serialize(tx).expect("serializes");
    let size = data.len() as u64;
    Packet {
        data,
        meta: Some(Meta {
            size,
            addr: "".to_string(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    }
}

/// Perform the ed25519 challenge/response handshake and return an access token.
/// Mirrors the canonical jito-relayer client: sign `pubkey-challenge`.
async fn authenticate(auth_url: String, kp: &Keypair) -> String {
    let mut client = AuthServiceClient::connect(auth_url)
        .await
        .expect("connect to auth service");

    let challenge = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: 1, // SEARCHER
            pubkey: kp.pubkey().to_bytes().to_vec(),
        })
        .await
        .expect("generate challenge")
        .into_inner()
        .challenge;

    let full_challenge = format!("{}-{}", kp.pubkey(), challenge);
    let signed = kp.sign_message(full_challenge.as_bytes()).as_ref().to_vec();

    let tokens = client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: full_challenge,
            client_pubkey: kp.pubkey().to_bytes().to_vec(),
            signed_challenge: signed,
        })
        .await
        .expect("generate tokens")
        .into_inner();

    tokens
        .access_token
        .expect("access token present")
        .value
}

async fn request_and_confirm_airdrop(client: &RpcClient, pubkeys: &[Pubkey]) -> bool {
    let mut sigs = Vec::new();

    info!("requesting airdrop pubkeys: {:?}", pubkeys);

    for pubkey in pubkeys {
        let signature = client
            .request_airdrop(pubkey, 100000000000)
            .await
            .expect("gets signature");
        sigs.push(signature);
    }

    let now = Instant::now();
    while now.elapsed() < Duration::from_secs(20) {
        let r = client
            .get_signature_statuses(&sigs)
            .await
            .expect("got statuses");
        if r.value.iter().all(|s| s.is_some()) {
            info!("got airdrop pubkeys: {:?}", pubkeys);
            return true;
        }
    }
    false
}

fn main() {
    env_logger::init();

    let args: Args = Args::parse();

    let kp = Arc::new(read_keypair_file(&args.keypair_path).expect("failed to read keypair file"));
    let rpc_client = RpcClient::new(args.rpc_url);

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        // Authenticate first, then attach the bearer token to every searcher RPC.
        let access_token = authenticate(args.auth_service_url, &kp).await;
        info!("authenticated with block engine");
        let bearer: MetadataValue<_> = format!("Bearer {access_token}")
            .parse()
            .expect("valid bearer header");

        let channel = Channel::from_shared(args.searcher_service_url)
            .expect("valid searcher url")
            .connect()
            .await
            .expect("connect to searcher service");
        let mut searcher_client =
            SearcherServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
                req.metadata_mut()
                    .insert("authorization", bearer.clone());
                Ok(req)
            });

        if !request_and_confirm_airdrop(&rpc_client, &[kp.pubkey()]).await {
            error!("error requesting airdrop");
            exit(1);
        }
        sleep(Duration::from_secs(5)).await;

        let mut last_blockhash_time = Instant::now();
        let mut blockhash = rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig {
                commitment: CommitmentLevel::Processed,
            })
            .await
            .expect("latest blockhash")
            .0;
        let mut base = 0;

        info!("sending bundles...");
        loop {
            if last_blockhash_time.elapsed() > Duration::from_secs(5) {
                blockhash = rpc_client
                    .get_latest_blockhash_with_commitment(CommitmentConfig {
                        commitment: CommitmentLevel::Processed,
                    })
                    .await
                    .expect("latest blockhash")
                    .0;
                last_blockhash_time = Instant::now();
            }
            let txs: Vec<_> = (0..5)
                .map(|amount| {
                    VersionedTransaction::from(system_transaction::transfer(
                        &kp,
                        &kp.pubkey(),
                        base + amount,
                        blockhash,
                    ))
                })
                .collect();
            base += txs.len() as u64;

            let result = searcher_client
                .send_bundle(SendBundleRequest {
                    bundle: Some(Bundle {
                        header: Some(Header {
                            ts: Some(prost_types::Timestamp::from(SystemTime::now())),
                        }),
                        packets: txs.iter().map(proto_packet_from_versioned_tx).collect(),
                    }),
                })
                .await;
            info!("uuid: {:?}", result.unwrap().into_inner().uuid);
            sleep(Duration::from_millis(1)).await;
        }
    });
}
