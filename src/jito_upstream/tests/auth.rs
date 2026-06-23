use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use jito_auth::server::AuthServiceImpl;
use jito_auth::token::AuthState;
use jito_protos::auth::auth_service_server::AuthServiceServer;
use jito_upstream::auth::TokenManager;
use jito_upstream::tls::connect;
use solana_sdk::signature::{Keypair, Signer};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test]
async fn authenticates_as_validator_and_exposes_token() {
    let keypair = Arc::new(Keypair::new());
    let mut allowed = HashSet::new();
    allowed.insert(keypair.pubkey().to_string());
    let state = Arc::new(AuthState::new(b"test-secret".to_vec(), Some(allowed)));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AuthServiceServer::new(AuthServiceImpl::new(server_state)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channel = connect(format!("http://{addr}")).await.unwrap();
    let manager = TokenManager::start(channel, keypair.clone()).await.unwrap();

    let token = manager.token();
    assert!(!token.is_empty());

    let claims = state.validate(&token).expect("issued token validates");
    assert_eq!(claims.sub, keypair.pubkey().to_string());
    assert_eq!(claims.role, 2);
    assert!(!claims.refresh);
    assert_eq!(manager.bearer(), format!("Bearer {token}"));
}

#[tokio::test]
async fn rejects_non_whitelisted_validator() {
    let keypair = Arc::new(Keypair::new());
    let state = Arc::new(AuthState::new(b"test-secret".to_vec(), Some(HashSet::new())));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AuthServiceServer::new(AuthServiceImpl::new(state)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channel = connect(format!("http://{addr}")).await.unwrap();
    assert!(TokenManager::start(channel, keypair).await.is_err());
}

#[tokio::test]
async fn keeps_token_fresh_via_refresh() {
    let keypair = Arc::new(Keypair::new());
    let mut allowed = HashSet::new();
    allowed.insert(keypair.pubkey().to_string());
    let mut state = AuthState::new(b"test-secret".to_vec(), Some(allowed));
    state.access_ttl = Duration::from_secs(2);
    let state = Arc::new(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        Server::builder()
            .add_service(AuthServiceServer::new(AuthServiceImpl::new(server_state)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channel = connect(format!("http://{addr}")).await.unwrap();
    let manager = TokenManager::start(channel, keypair).await.unwrap();

    let first_exp = state.validate(&manager.token()).unwrap().exp;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let later = state.validate(&manager.token()).unwrap();

    assert!(!later.refresh);
    assert!(
        later.exp > first_exp,
        "refresh should advance the access token expiry ({} <= {first_exp})",
        later.exp
    );
}
