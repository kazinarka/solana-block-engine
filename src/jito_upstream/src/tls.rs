use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub async fn connect(url: String) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(url)?.connect().await
}

pub async fn connect_tls(url: String) -> Result<Channel, tonic::transport::Error> {
    Endpoint::from_shared(url)?
        .tls_config(ClientTlsConfig::new().with_webpki_roots())?
        .connect()
        .await
}
