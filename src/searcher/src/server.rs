use std::sync::Arc;

use jito_auction::Auction;
use jito_protos::bundle::{BundleResult, BundleUuid};
use jito_protos::searcher::{
    searcher_service_server::SearcherService, ConnectedLeadersRegionedRequest,
    ConnectedLeadersRegionedResponse, ConnectedLeadersRequest, ConnectedLeadersResponse,
    GetRegionsRequest, GetRegionsResponse, GetTipAccountsRequest, GetTipAccountsResponse,
    NextScheduledLeaderRequest, NextScheduledLeaderResponse, SendBundleRequest, SendBundleResponse,
    SubscribeBundleResultsRequest,
};
use log::info;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use uuid::Uuid;

pub struct SearcherServiceImpl {
    auction: Arc<Auction>,
}

impl SearcherServiceImpl {
    pub const MAX_BUNDLE_LEN: usize = 5;

    pub fn new(auction: Arc<Auction>) -> Self {
        SearcherServiceImpl { auction }
    }
}

#[tonic::async_trait]
impl SearcherService for SearcherServiceImpl {
    // The pending-tx mempool subscription was removed from the protocol; the
    // current surface streams *bundle results* back to searchers instead.
    type SubscribeBundleResultsStream = ReceiverStream<Result<BundleResult, Status>>;

    async fn subscribe_bundle_results(
        &self,
        _request: Request<SubscribeBundleResultsRequest>,
    ) -> Result<Response<Self::SubscribeBundleResultsStream>, Status> {
        // TODO: stream landed/dropped results once the auction tracks outcomes.
        unimplemented!()
    }

    async fn send_bundle(
        &self,
        request: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        let uuid = Uuid::new_v4().to_string();
        let bundle_uuid = BundleUuid {
            bundle: request.into_inner().bundle,
            uuid: uuid.clone(),
        };

        info!("received bundle_uuid: {:?}", bundle_uuid.uuid);

        // Hand the bundle to the auction; it will be scored and compete for
        // inclusion on the next auction tick (rather than forwarded immediately).
        if bundle_uuid.bundle.is_some() {
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
