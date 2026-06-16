//! Shared auth state: the challenge store and JWT issue/verify logic.
//!
//! This is deliberately self-contained (no Solana runtime dependency). It is
//! shared in-process between the `AuthService` (which issues tokens) and the
//! `AuthInterceptor` (which validates them on the validator/relayer/searcher
//! services), so both halves agree on the same HS256 secret.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tonic::Status;

/// JWT claims embedded in access/refresh tokens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Base58 pubkey of the authenticated client.
    pub sub: String,
    /// Role from the auth protocol (RELAYER=0, SEARCHER=1, VALIDATOR=2, ...).
    pub role: i32,
    /// `true` for refresh tokens, `false` for access tokens.
    pub refresh: bool,
    /// Expiry, seconds since unix epoch. `jsonwebtoken` validates this.
    pub exp: usize,
}

struct ChallengeEntry {
    challenge: String,
    expires_at: Instant,
}

pub struct AuthState {
    jwt_secret: Vec<u8>,
    /// Pending challenges keyed by base58 client pubkey.
    challenges: Mutex<HashMap<String, ChallengeEntry>>,
    /// If `Some`, only these base58 pubkeys may authenticate. If `None`, any
    /// pubkey is allowed (logged as a warning at startup).
    allowed_pubkeys: Option<HashSet<String>>,
    pub access_ttl: Duration,
    pub refresh_ttl: Duration,
    challenge_ttl: Duration,
}

impl AuthState {
    pub fn new(jwt_secret: Vec<u8>, allowed_pubkeys: Option<HashSet<String>>) -> Self {
        Self {
            jwt_secret,
            challenges: Mutex::new(HashMap::new()),
            allowed_pubkeys,
            access_ttl: Duration::from_secs(30 * 60),
            refresh_ttl: Duration::from_secs(24 * 60 * 60),
            challenge_ttl: Duration::from_secs(30),
        }
    }

    pub fn is_allowed(&self, pubkey_b58: &str) -> bool {
        match &self.allowed_pubkeys {
            Some(set) => set.contains(pubkey_b58),
            None => true,
        }
    }

    /// Generate, store, and return a fresh challenge nonce for a pubkey.
    pub fn create_challenge(&self, pubkey_b58: &str) -> String {
        let challenge = uuid::Uuid::new_v4().to_string();
        let mut map = self.challenges.lock().unwrap();
        map.insert(
            pubkey_b58.to_string(),
            ChallengeEntry {
                challenge: challenge.clone(),
                expires_at: Instant::now() + self.challenge_ttl,
            },
        );
        challenge
    }

    /// Consume (remove) a non-expired challenge for a pubkey, if present.
    pub fn take_challenge(&self, pubkey_b58: &str) -> Option<String> {
        let mut map = self.challenges.lock().unwrap();
        match map.remove(pubkey_b58) {
            Some(entry) if entry.expires_at > Instant::now() => Some(entry.challenge),
            _ => None,
        }
    }

    /// Drop all expired challenges (called periodically to bound memory).
    pub fn prune_challenges(&self) {
        let now = Instant::now();
        self.challenges
            .lock()
            .unwrap()
            .retain(|_, e| e.expires_at > now);
    }

    /// Issue a signed JWT. Returns `(token, expires_at_unix_secs)`.
    pub fn issue(&self, pubkey_b58: &str, role: i32, refresh: bool) -> Result<(String, i64), Status> {
        let ttl = if refresh { self.refresh_ttl } else { self.access_ttl };
        let exp = (now_unix() + ttl.as_secs()) as usize;
        let claims = Claims {
            sub: pubkey_b58.to_string(),
            role,
            refresh,
            exp,
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(&self.jwt_secret),
        )
        .map_err(|e| Status::internal(format!("token signing failed: {e}")))?;
        Ok((token, exp as i64))
    }

    /// Validate a JWT (signature + expiry) and return its claims.
    pub fn validate(&self, token: &str) -> Result<Claims, Status> {
        let data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(&self.jwt_secret),
            &Validation::new(Algorithm::HS256),
        )
        .map_err(|e| Status::unauthenticated(format!("invalid token: {e}")))?;
        Ok(data.claims)
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}
