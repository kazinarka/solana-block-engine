use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, GenerateAuthTokensResponse,
    RefreshAccessTokenRequest, Role, Token,
};
use log::{error, warn};
use solana_sdk::signature::{Keypair, Signer};
use tonic::transport::Channel;

const REFRESH_MARGIN: Duration = Duration::from_secs(60);
const RETRY_DELAY: Duration = Duration::from_secs(5);

pub struct TokenManager {
    token: RwLock<String>,
}

impl TokenManager {
    pub async fn start(
        channel: Channel,
        keypair: Arc<Keypair>,
    ) -> Result<Arc<Self>, tonic::Status> {
        let mut client = AuthServiceClient::new(channel);
        let tokens = handshake(&mut client, &keypair).await?;
        let access = token_value(&tokens.access_token)?;
        let manager = Arc::new(Self {
            token: RwLock::new(access),
        });
        tokio::spawn(refresh_loop(client, keypair, manager.clone(), tokens));
        Ok(manager)
    }

    pub fn token(&self) -> String {
        self.token.read().unwrap().clone()
    }

    pub fn bearer(&self) -> String {
        format!("Bearer {}", self.token())
    }

    fn set(&self, value: String) {
        *self.token.write().unwrap() = value;
    }
}

async fn handshake(
    client: &mut AuthServiceClient<Channel>,
    keypair: &Keypair,
) -> Result<GenerateAuthTokensResponse, tonic::Status> {
    let pubkey = keypair.pubkey();
    let challenge = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Validator as i32,
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await?
        .into_inner()
        .challenge;

    let full = format!("{}-{}", pubkey, challenge);
    let signed = keypair.sign_message(full.as_bytes()).as_ref().to_vec();

    let tokens = client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: full,
            client_pubkey: pubkey.to_bytes().to_vec(),
            signed_challenge: signed,
        })
        .await?
        .into_inner();
    Ok(tokens)
}

async fn refresh_loop(
    mut client: AuthServiceClient<Channel>,
    keypair: Arc<Keypair>,
    manager: Arc<TokenManager>,
    mut tokens: GenerateAuthTokensResponse,
) {
    loop {
        let wait = ttl_until(&tokens.access_token).saturating_sub(REFRESH_MARGIN);
        tokio::time::sleep(wait).await;

        let refreshed = if ttl_until(&tokens.refresh_token) > REFRESH_MARGIN {
            match token_value(&tokens.refresh_token) {
                Ok(refresh_token) => {
                    match client
                        .refresh_access_token(RefreshAccessTokenRequest { refresh_token })
                        .await
                    {
                        Ok(resp) => Some(resp.into_inner().access_token),
                        Err(e) => {
                            warn!("upstream token refresh failed: {e}");
                            None
                        }
                    }
                }
                Err(_) => None,
            }
        } else {
            None
        };

        match refreshed {
            Some(access) => {
                if let Ok(value) = token_value(&access) {
                    manager.set(value);
                }
                tokens.access_token = access;
            }
            None => loop {
                match handshake(&mut client, &keypair).await {
                    Ok(new) => {
                        if let Ok(value) = token_value(&new.access_token) {
                            manager.set(value);
                        }
                        tokens = new;
                        break;
                    }
                    Err(e) => {
                        error!("upstream re-authentication failed: {e}");
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                }
            },
        }
    }
}

fn token_value(token: &Option<Token>) -> Result<String, tonic::Status> {
    token
        .as_ref()
        .map(|t| t.value.clone())
        .ok_or_else(|| tonic::Status::internal("auth response missing token"))
}

fn ttl_until(token: &Option<Token>) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let exp = token
        .as_ref()
        .and_then(|t| t.expires_at_utc.as_ref())
        .map(|ts| ts.seconds)
        .unwrap_or(now);
    Duration::from_secs((exp - now).max(0) as u64)
}
