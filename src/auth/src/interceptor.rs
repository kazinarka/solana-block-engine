//! gRPC interceptor that enforces a valid bearer token on every request to the
//! validator / relayer / searcher services. The auth service itself is *not*
//! wrapped with this — clients must be able to call it without a token to
//! obtain one.

use std::sync::Arc;

use tonic::service::Interceptor;
use tonic::{Request, Status};

use crate::token::{AuthState, Claims};

#[derive(Clone)]
pub struct AuthInterceptor {
    state: Arc<AuthState>,
    /// If `Some`, the token's role must equal this (RELAYER=0, SEARCHER=1,
    /// VALIDATOR=2). If `None`, any authenticated role is accepted.
    required_role: Option<i32>,
}

impl AuthInterceptor {
    /// Accept any authenticated client regardless of role.
    pub fn new(state: Arc<AuthState>) -> Self {
        Self {
            state,
            required_role: None,
        }
    }

    /// Accept only clients whose token carries `role`.
    pub fn for_role(state: Arc<AuthState>, role: i32) -> Self {
        Self {
            state,
            required_role: Some(role),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization header"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("invalid authorization header"))?;

        let token = header
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization must be 'Bearer <token>'"))?;

        let claims = self.state.validate(token)?;
        if claims.refresh {
            // A refresh token must never be accepted as an access credential.
            return Err(Status::unauthenticated("refresh token not valid for access"));
        }

        if let Some(required) = self.required_role {
            if claims.role != required {
                return Err(Status::permission_denied(format!(
                    "token role {} not permitted on this service (requires role {required})",
                    claims.role
                )));
            }
        }

        // Stash claims so handlers can read the caller's pubkey/role if needed.
        request.extensions_mut().insert::<Claims>(claims);
        Ok(request)
    }
}
