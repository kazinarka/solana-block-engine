//! End-to-end test of the auth handshake, driving the gRPC service methods the
//! same way the real jito-relayer client does: generate challenge → sign
//! `pubkey-challenge` with an ed25519 key → exchange for tokens → use/refresh.

use std::collections::HashSet;
use std::sync::Arc;

use ed25519_dalek::{Signer, SigningKey};
use jito_auth::interceptor::AuthInterceptor;
use jito_auth::server::AuthServiceImpl;
use jito_auth::token::AuthState;
use jito_protos::auth::auth_service_server::AuthService;
use jito_protos::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest,
};
use tonic::service::Interceptor;
use tonic::Request;

fn test_keypair() -> (SigningKey, [u8; 32], String) {
    let sk = SigningKey::from_bytes(&[7u8; 32]);
    let pubkey = sk.verifying_key().to_bytes();
    let pubkey_b58 = bs58::encode(pubkey).into_string();
    (sk, pubkey, pubkey_b58)
}

/// Run the full handshake and return the issued access + refresh token values.
async fn do_handshake(svc: &AuthServiceImpl, sk: &SigningKey, pubkey: [u8; 32]) -> (String, String) {
    let pubkey_b58 = bs58::encode(pubkey).into_string();

    let challenge = svc
        .generate_auth_challenge(Request::new(GenerateAuthChallengeRequest {
            role: 2, // VALIDATOR
            pubkey: pubkey.to_vec(),
        }))
        .await
        .expect("challenge")
        .into_inner()
        .challenge;

    // Client signs the challenge prepended with its pubkey.
    let full = format!("{pubkey_b58}-{challenge}");
    let sig = sk.sign(full.as_bytes()).to_bytes().to_vec();

    let tokens = svc
        .generate_auth_tokens(Request::new(GenerateAuthTokensRequest {
            challenge: full,
            client_pubkey: pubkey.to_vec(),
            signed_challenge: sig,
        }))
        .await
        .expect("tokens")
        .into_inner();

    (
        tokens.access_token.unwrap().value,
        tokens.refresh_token.unwrap().value,
    )
}

fn state_allowing(pubkey_b58: &str) -> Arc<AuthState> {
    let mut allowed = HashSet::new();
    allowed.insert(pubkey_b58.to_string());
    Arc::new(AuthState::new(b"test-secret".to_vec(), Some(allowed)))
}

#[tokio::test]
async fn full_handshake_issues_usable_access_token() {
    let (sk, pubkey, pubkey_b58) = test_keypair();
    let state = state_allowing(&pubkey_b58);
    let svc = AuthServiceImpl::new(state.clone());

    let (access, refresh) = do_handshake(&svc, &sk, pubkey).await;

    // Access token validates and is not a refresh token.
    let claims = state.validate(&access).expect("access valid");
    assert_eq!(claims.sub, pubkey_b58);
    assert!(!claims.refresh);

    // Refresh token validates and is marked as refresh.
    let rclaims = state.validate(&refresh).expect("refresh valid");
    assert!(rclaims.refresh);

    // The interceptor accepts the access token...
    let mut interceptor = AuthInterceptor::new(state.clone());
    let mut req = Request::new(());
    req.metadata_mut()
        .insert("authorization", format!("Bearer {access}").parse().unwrap());
    assert!(interceptor.call(req).is_ok());

    // ...but rejects the refresh token used as an access credential.
    let mut req = Request::new(());
    req.metadata_mut()
        .insert("authorization", format!("Bearer {refresh}").parse().unwrap());
    assert!(interceptor.call(req).is_err());

    // ...and rejects requests with no token.
    assert!(interceptor.call(Request::new(())).is_err());
}

#[tokio::test]
async fn interceptor_enforces_role() {
    let (sk, pubkey, pubkey_b58) = test_keypair();
    let state = state_allowing(&pubkey_b58);
    let svc = AuthServiceImpl::new(state.clone());

    // do_handshake authenticates as role 2 (VALIDATOR).
    let (access, _refresh) = do_handshake(&svc, &sk, pubkey).await;

    // A VALIDATOR-scoped interceptor accepts the token...
    let mut validator_ic = AuthInterceptor::for_role(state.clone(), 2);
    let mut req = Request::new(());
    req.metadata_mut()
        .insert("authorization", format!("Bearer {access}").parse().unwrap());
    assert!(validator_ic.call(req).is_ok(), "validator token on validator service");

    // ...but a SEARCHER-scoped interceptor rejects it (role mismatch), proving
    // the requested role propagates from challenge into the issued token.
    let mut searcher_ic = AuthInterceptor::for_role(state.clone(), 1);
    let mut req = Request::new(());
    req.metadata_mut()
        .insert("authorization", format!("Bearer {access}").parse().unwrap());
    assert!(searcher_ic.call(req).is_err(), "validator token rejected on searcher service");
}

#[tokio::test]
async fn refresh_token_yields_new_access_token() {
    let (sk, pubkey, pubkey_b58) = test_keypair();
    let state = state_allowing(&pubkey_b58);
    let svc = AuthServiceImpl::new(state.clone());

    let (_access, refresh) = do_handshake(&svc, &sk, pubkey).await;

    let new_access = svc
        .refresh_access_token(Request::new(RefreshAccessTokenRequest {
            refresh_token: refresh,
        }))
        .await
        .expect("refresh ok")
        .into_inner()
        .access_token
        .unwrap()
        .value;

    let claims = state.validate(&new_access).expect("new access valid");
    assert_eq!(claims.sub, pubkey_b58);
    assert!(!claims.refresh);
}

#[tokio::test]
async fn wrong_signature_is_rejected() {
    let (_sk, pubkey, pubkey_b58) = test_keypair();
    let state = state_allowing(&pubkey_b58);
    let svc = AuthServiceImpl::new(state);

    let challenge = svc
        .generate_auth_challenge(Request::new(GenerateAuthChallengeRequest {
            role: 2,
            pubkey: pubkey.to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .challenge;

    // Sign with a DIFFERENT key than the claimed pubkey.
    let attacker = SigningKey::from_bytes(&[9u8; 32]);
    let full = format!("{pubkey_b58}-{challenge}");
    let sig = attacker.sign(full.as_bytes()).to_bytes().to_vec();

    let result = svc
        .generate_auth_tokens(Request::new(GenerateAuthTokensRequest {
            challenge: full,
            client_pubkey: pubkey.to_vec(),
            signed_challenge: sig,
        }))
        .await;
    assert!(result.is_err(), "forged signature must be rejected");
}

#[tokio::test]
async fn non_whitelisted_pubkey_is_rejected() {
    let (_sk, pubkey, _pubkey_b58) = test_keypair();
    // Whitelist a DIFFERENT pubkey.
    let state = state_allowing("SomeOtherPubkey1111111111111111111111111111");
    let svc = AuthServiceImpl::new(state);

    let result = svc
        .generate_auth_challenge(Request::new(GenerateAuthChallengeRequest {
            role: 2,
            pubkey: pubkey.to_vec(),
        }))
        .await;
    assert!(result.is_err(), "non-whitelisted pubkey must be denied");
}
