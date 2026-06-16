//! Real `AuthService`: ed25519 challenge/response handshake + HS256 JWT issuance.
//!
//! Handshake (matches the canonical jito-relayer client in
//! `block_engine/src/block_engine.rs`):
//!   1. client → GenerateAuthChallenge { role, pubkey }
//!      server → { challenge }                              (random nonce, stored)
//!   2. client signs `format!("{pubkey_base58}-{challenge}")` with its ed25519 key
//!      client → GenerateAuthTokens { challenge: <full string>, client_pubkey, signed_challenge }
//!      server verifies the signature, then issues access + refresh JWTs
//!   3. client → RefreshAccessToken { refresh_token } → fresh access token

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use jito_protos::auth::{
    auth_service_server::AuthService, GenerateAuthChallengeRequest, GenerateAuthChallengeResponse,
    GenerateAuthTokensRequest, GenerateAuthTokensResponse, RefreshAccessTokenRequest,
    RefreshAccessTokenResponse, Token as PbToken,
};
use log::*;
use tonic::{Request, Response, Status};

use crate::token::AuthState;

pub struct AuthServiceImpl {
    state: Arc<AuthState>,
}

impl AuthServiceImpl {
    pub fn new(state: Arc<AuthState>) -> Self {
        AuthServiceImpl { state }
    }
}

/// Convert a 32-byte pubkey into its base58 form (matches `solana_sdk::Pubkey`'s
/// `Display`, which is what the client uses when building the challenge string).
fn pubkey_b58(bytes: &[u8]) -> Result<String, Status> {
    if bytes.len() != 32 {
        return Err(Status::invalid_argument("pubkey must be 32 bytes"));
    }
    Ok(bs58::encode(bytes).into_string())
}

fn pb_token(value: String, exp_unix: i64) -> PbToken {
    PbToken {
        value,
        expires_at_utc: Some(prost_types::Timestamp {
            seconds: exp_unix,
            nanos: 0,
        }),
    }
}

#[tonic::async_trait]
impl AuthService for AuthServiceImpl {
    async fn generate_auth_challenge(
        &self,
        req: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        let inner = req.into_inner();
        let pubkey = pubkey_b58(&inner.pubkey)?;

        if !self.state.is_allowed(&pubkey) {
            warn!("rejected challenge from non-whitelisted pubkey {pubkey}");
            return Err(Status::permission_denied(
                "pubkey is not authorized to connect to this block engine",
            ));
        }

        let challenge = self.state.create_challenge(&pubkey, inner.role);
        info!("issued auth challenge to {pubkey} (role={})", inner.role);
        Ok(Response::new(GenerateAuthChallengeResponse { challenge }))
    }

    async fn generate_auth_tokens(
        &self,
        req: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        let inner = req.into_inner();
        let pubkey = pubkey_b58(&inner.client_pubkey)?;

        if !self.state.is_allowed(&pubkey) {
            return Err(Status::permission_denied("pubkey is not authorized"));
        }

        // Look up (and consume) the challenge we previously issued, along with
        // the role it was requested for.
        let (stored, role) = self.state.take_challenge(&pubkey).ok_or_else(|| {
            Status::permission_denied(
                "no active challenge; call GenerateAuthChallenge first (or it expired)",
            )
        })?;

        // The client signs the challenge *prepended with its pubkey*. Rebuild
        // that exact string and require the request to match it.
        let expected = format!("{pubkey}-{stored}");
        if expected != inner.challenge {
            return Err(Status::invalid_argument(
                "submitted challenge does not match the issued challenge",
            ));
        }

        // Verify the ed25519 signature over the full challenge string.
        let vk_bytes: [u8; 32] = inner
            .client_pubkey
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("pubkey must be 32 bytes"))?;
        let verifying_key = VerifyingKey::from_bytes(&vk_bytes)
            .map_err(|_| Status::invalid_argument("invalid ed25519 pubkey"))?;

        let sig_bytes: [u8; 64] = inner
            .signed_challenge
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("signature must be 64 bytes"))?;
        let signature = Signature::from_bytes(&sig_bytes);

        verifying_key
            .verify(inner.challenge.as_bytes(), &signature)
            .map_err(|_| Status::permission_denied("challenge signature verification failed"))?;

        // Issue tokens carrying the role the client authenticated for; the
        // per-service interceptors enforce it.
        let (access, access_exp) = self.state.issue(&pubkey, role, false)?;
        let (refresh, refresh_exp) = self.state.issue(&pubkey, role, true)?;
        info!("authenticated {pubkey}; issued access + refresh tokens");

        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(pb_token(access, access_exp)),
            refresh_token: Some(pb_token(refresh, refresh_exp)),
        }))
    }

    async fn refresh_access_token(
        &self,
        req: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        let inner = req.into_inner();
        let claims = self.state.validate(&inner.refresh_token)?;
        if !claims.refresh {
            return Err(Status::permission_denied(
                "supplied token is not a refresh token",
            ));
        }
        let (access, access_exp) = self.state.issue(&claims.sub, claims.role, false)?;
        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(pb_token(access, access_exp)),
        }))
    }
}

/// Generate a random 32-byte HS256 secret at startup (when none is configured).
pub fn random_secret() -> Vec<u8> {
    // Two v4 UUIDs give 32 random bytes without adding a separate RNG crate.
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes
}

/// Spawn a background task that periodically prunes expired challenges.
pub fn spawn_challenge_pruner(state: Arc<AuthState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            state.prune_challenges();
        }
    });
}
