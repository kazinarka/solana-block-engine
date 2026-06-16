//! Generated gRPC bindings for the Jito MEV protocol (vendored from
//! github.com/jito-labs/mev-protos). This crate is intentionally lightweight —
//! it has no Solana dependency so the rest of the workspace can compile and
//! test the wire protocol without pulling the full validator dependency tree.

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod block {
    tonic::include_proto!("block");
}

pub mod block_engine {
    tonic::include_proto!("block_engine");
}

pub mod bundle {
    tonic::include_proto!("bundle");
}

pub mod packet {
    tonic::include_proto!("packet");
}

pub mod relayer {
    tonic::include_proto!("relayer");
}

pub mod searcher {
    tonic::include_proto!("searcher");
}

pub mod shared {
    tonic::include_proto!("shared");
}
