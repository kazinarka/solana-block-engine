//! Test harness: a VALIDATOR-role client that authenticates with the block
//! engine and subscribes to the bundle stream, printing each bundle it receives.
//! Stands in for a jito-solana validator in end-to-end testing.

use std::time::Duration;

use clap::Parser;
use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::{GenerateAuthChallengeRequest, GenerateAuthTokensRequest};
use jito_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use jito_protos::block_engine::SubscribeBundlesRequest;
use log::{error, info};
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use tokio::runtime::Builder;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, env, default_value_t = String::from("http://localhost:1003"))]
    validator_service_url: String,

    #[clap(long, env, default_value_t = String::from("http://localhost:1005"))]
    auth_service_url: String,

    #[clap(long, env, default_value_t = String::from("./keypair.json"))]
    keypair_path: String,
}

async fn authenticate(auth_url: String, kp: &Keypair) -> String {
    let mut client = AuthServiceClient::connect(auth_url)
        .await
        .expect("connect to auth service");
    let challenge = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: 2, // VALIDATOR
            pubkey: kp.pubkey().to_bytes().to_vec(),
        })
        .await
        .expect("generate challenge")
        .into_inner()
        .challenge;
    let full = format!("{}-{}", kp.pubkey(), challenge);
    let signed = kp.sign_message(full.as_bytes()).as_ref().to_vec();
    client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: full,
            client_pubkey: kp.pubkey().to_bytes().to_vec(),
            signed_challenge: signed,
        })
        .await
        .expect("generate tokens")
        .into_inner()
        .access_token
        .expect("access token")
        .value
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let kp = read_keypair_file(&args.keypair_path).expect("read keypair");

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        let access_token = authenticate(args.auth_service_url, &kp).await;
        info!("validator authenticated as {}", kp.pubkey());
        let bearer: MetadataValue<_> = format!("Bearer {access_token}").parse().unwrap();

        let channel = Channel::from_shared(args.validator_service_url)
            .expect("valid url")
            .connect()
            .await
            .expect("connect to validator service");
        let mut client =
            BlockEngineValidatorClient::with_interceptor(channel, move |mut req: Request<()>| {
                req.metadata_mut().insert("authorization", bearer.clone());
                Ok(req)
            });

        let mut stream = client
            .subscribe_bundles(SubscribeBundlesRequest {})
            .await
            .expect("subscribe_bundles")
            .into_inner();
        info!("subscribed to bundles; waiting...");

        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    for b in resp.bundles {
                        let n = b.bundle.map(|bb| bb.packets.len()).unwrap_or(0);
                        info!("RECEIVED bundle uuid={} ({n} txs)", b.uuid);
                    }
                }
                Ok(None) => {
                    info!("bundle stream closed");
                    break;
                }
                Err(e) => {
                    error!("bundle stream error: {e}");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
}
