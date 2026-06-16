use std::sync::Arc;

use jito_auction::Auction;
use jito_auth::token::Claims;
use jito_interest::InterestRegistry;
use jito_protos::bundle::{BundleResult, BundleUuid};
use jito_protos::searcher::{
    searcher_service_server::SearcherService, ConnectedLeadersRegionedRequest,
    ConnectedLeadersRegionedResponse, ConnectedLeadersRequest, ConnectedLeadersResponse,
    GetRegionsRequest, GetRegionsResponse, GetTipAccountsRequest, GetTipAccountsResponse,
    NextScheduledLeaderRequest, NextScheduledLeaderResponse, SendBundleRequest, SendBundleResponse,
    SubscribeBundleResultsRequest,
};
use jito_results::BundleResults;
use log::info;
use tokio::sync::mpsc::channel;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Authenticated searcher identity (base58 pubkey) from the auth interceptor.
fn owner_of<T>(req: &Request<T>) -> String {
    req.extensions()
        .get::<Claims>()
        .map(|c| c.sub.clone())
        .unwrap_or_default()
}

pub struct SearcherServiceImpl {
    auction: Arc<Auction>,
    interest: Arc<InterestRegistry>,
    results: Arc<BundleResults>,
}

impl SearcherServiceImpl {
    pub const MAX_BUNDLE_LEN: usize = 5;

    pub fn new(
        auction: Arc<Auction>,
        interest: Arc<InterestRegistry>,
        results: Arc<BundleResults>,
    ) -> Self {
        SearcherServiceImpl {
            auction,
            interest,
            results,
        }
    }
}

#[tonic::async_trait]
impl SearcherService for SearcherServiceImpl {
    // The pending-tx mempool subscription was removed from the protocol; the
    // current surface streams *bundle results* back to searchers instead.
    type SubscribeBundleResultsStream = ReceiverStream<Result<BundleResult, Status>>;

    async fn subscribe_bundle_results(
        &self,
        request: Request<SubscribeBundleResultsRequest>,
    ) -> Result<Response<Self::SubscribeBundleResultsStream>, Status> {
        let owner = owner_of(&request);
        info!("searcher {owner} subscribed to bundle results");
        let (sender, receiver) = channel(256);
        self.results.add_subscriber(&owner, sender);
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn send_bundle(
        &self,
        request: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        let owner = owner_of(&request);
        let uuid = Uuid::new_v4().to_string();
        let bundle_uuid = BundleUuid {
            bundle: request.into_inner().bundle,
            uuid: uuid.clone(),
        };

        info!("received bundle_uuid: {:?}", bundle_uuid.uuid);

        // Associate the bundle with its searcher (for result routing), record
        // its writable accounts/programs for the relayer, then run the auction.
        if bundle_uuid.bundle.is_some() {
            self.results.register(&uuid, &owner);
            self.interest.observe_bundle(&bundle_uuid);
            self.auction.submit(bundle_uuid);
        }

        Ok(Response::new(SendBundleResponse { uuid }))
    }

    async fn get_next_scheduled_leader(
        &self,
        _request: Request<NextScheduledLeaderRequest>,
    ) -> Result<Response<NextScheduledLeaderResponse>, Status> {
        unimplemented!()
    }

    async fn get_connected_leaders(
        &self,
        _request: Request<ConnectedLeadersRequest>,
    ) -> Result<Response<ConnectedLeadersResponse>, Status> {
        unimplemented!()
    }

    async fn get_connected_leaders_regioned(
        &self,
        _request: Request<ConnectedLeadersRegionedRequest>,
    ) -> Result<Response<ConnectedLeadersRegionedResponse>, Status> {
        unimplemented!()
    }

    async fn get_tip_accounts(
        &self,
        _request: Request<GetTipAccountsRequest>,
    ) -> Result<Response<GetTipAccountsResponse>, Status> {
        unimplemented!()
    }

    async fn get_regions(
        &self,
        _request: Request<GetRegionsRequest>,
    ) -> Result<Response<GetRegionsResponse>, Status> {
        unimplemented!()
    }
}
