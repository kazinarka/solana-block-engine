pub mod auth;
pub mod consumer;
pub mod tls;

use std::sync::Arc;

use solana_sdk::signature::Keypair;

pub fn read_identity(path: &str) -> std::io::Result<Arc<Keypair>> {
    let keypair = solana_sdk::signature::read_keypair_file(path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Arc::new(keypair))
}
