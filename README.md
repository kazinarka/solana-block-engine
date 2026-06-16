# Meridian Block Engine

A self-hosted Solana block engine, bootstrapped from Jito's open-source
[`block_engine_simple`](https://github.com/jito-labs/block_engine_simple) reference
and modernized to a current Rust / `tonic` 0.12 / `prost` 0.13 toolchain.

It speaks the public [Jito MEV protocol](https://github.com/jito-labs/mev-protos)
(vendored under `src/jito_protos/protos/`), so an unmodified jito-solana validator
and a Jito-style relayer can connect to it without knowing it isn't Jito's engine.

## Architecture

```
 Searchers ──SendBundle──────────────┐
                                      ▼
 Relayer ──StartExpiringPacketStream──► [ MERIDIAN BLOCK ENGINE ] ──► Validator (jito-solana)
   ▲                                      ├─ SearcherService   :1234     SubscribePackets
   └──SubscribeAccountsOfInterest─────────┤  RelayerService    :1004     SubscribeBundles
                                          ├─ AuthService       :1005
                                          └─ ValidatorService  :1003
```

Two channels stitch the services together (see `src/block_engine/src/main.rs`):

- `bundle_sender → bundle_receiver`: searcher submits a bundle → validator forwarder fans it out to subscribed validators.
- `packet_sender → packet_receiver`: relayer streams packets in → validator forwarder fans them out.

## Crate layout (`src/`)

| Crate | Role | Status |
|-------|------|--------|
| `jito_protos` | Generated gRPC bindings (vendored mev-protos) | ✅ modernized, builds |
| `relayer` | `BlockEngineRelayer` service — ingests packets from the relayer | ✅ **new** (reference never built this) |
| `validator` | `BlockEngineValidator` service — fans packets+bundles to validators | ✅ builds |
| `searcher` | `SearcherService` — accepts bundles | ⚠️ `send_bundle` works; rest `unimplemented!()` |
| `auth` | `AuthService` — ed25519 challenge/response + HS256 JWT, interceptor, pubkey allowlist | ✅ real, tested |
| `block_engine` | binary wiring all services together | ✅ builds |
| `searcher_client` | test "bundle blaster" (authenticates, then streams bundles) | ✅ ported to Agave 2.x; not in default build |

## Build & run

```bash
cargo build --release
RUST_LOG=info ./target/release/jito-block-engine
```

Default bind addresses (override via flags or env): searcher `:1234`,
validator `:1003`, relayer `:1004`, auth `:1005`.

## What this is NOT yet (the hard, closed-source parts Jito never published)

This is a wiring skeleton. The MEV "brain" is intentionally absent:

1. ~~**Real auth**~~ ✅ done — ed25519 challenge/response + HS256 JWT in
   `src/auth/`, enforced via an interceptor on the validator/relayer/searcher
   services, with a configurable pubkey allowlist (`--allowed-pubkeys`).
2. **The auction** — bundles are forwarded 1:1 immediately. A real engine
   buffers bundles, simulates them against bank state, and selects the
   highest-tip combination that fits the block CU limit.
3. **Bundle simulation** — replay bundles against a Solana bank (SVM) to verify
   success and compute actual tip value.
4. **Leader-aware routing** — currently fans out to *all* connected validators.
   Should track the leader schedule and target the current/upcoming leader.
5. **Accounts/Programs of Interest** — `src/relayer/src/server.rs` hard-codes
   `"*"` (forward everything). Should be derived from submitted bundles so the
   relayer only forwards transactions touching contended state.
6. **Expiry handling** — `expiry_ms` on incoming packet batches is ignored.

## Testing end-to-end

The default build excludes the heavy Solana client. Build the blaster explicitly:

```bash
cargo build -p jito-searcher-client
# pubkey must be in the engine's allowlist:
ALLOWED_PUBKEYS=$(solana-keygen pubkey ~/.config/solana/id.json) \
  AUTH_JWT_SECRET=dev ./target/release/jito-block-engine &
./target/debug/jito-searcher-client --keypair-path ~/.config/solana/id.json
```

The client authenticates via the ed25519 handshake, then streams bundles
(needs a local validator/RPC for the airdrop + blockhash steps).

## Next steps (see task list)

- Add leader-schedule tracking + targeted routing (stop fanning out to all validators).
- Add bundle simulation + auction (the core MEV brain).
- Per-role token enforcement (VALIDATOR tokens only on the validator service, etc.).

## Provenance

- Skeleton: [`jito-labs/block_engine_simple`](https://github.com/jito-labs/block_engine_simple) (Apache-2.0)
- Protocol: [`jito-labs/mev-protos`](https://github.com/jito-labs/mev-protos)
- Relayer reference (other side of the wire): [`jito-foundation/jito-relayer`](https://github.com/jito-foundation/jito-relayer)
