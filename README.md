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
| `auth` | `AuthService` — challenge/token issuance | ⚠️ **stub: returns hard-coded tokens, no crypto** |
| `block_engine` | binary wiring all services together | ✅ builds |
| `searcher_client` | test "bundle blaster" | ⛔ excluded — pins solana 1.14, needs Agave 2.x port |

## Build & run

```bash
cargo build --release
RUST_LOG=info ./target/release/jito-block-engine
```

Default bind addresses (override via flags or env): searcher `:1234`,
validator `:1003`, relayer `:1004`, auth `:1005`.

## What this is NOT yet (the hard, closed-source parts Jito never published)

This is a wiring skeleton. The MEV "brain" is intentionally absent:

1. **Real auth** — `src/auth/src/server.rs` returns the literal strings
   `"access_token"` / `"refresh_token"`. Needs ed25519 challenge/response +
   signed-token verification (mirror the client side in `jito-foundation/jito-relayer`).
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

## Next steps (see task list)

- Port `searcher_client` to Agave 2.x so you can blast test bundles end-to-end.
- Implement the real auth server.
- Add leader-schedule tracking + targeted routing.
- Add bundle simulation + auction.

## Provenance

- Skeleton: [`jito-labs/block_engine_simple`](https://github.com/jito-labs/block_engine_simple) (Apache-2.0)
- Protocol: [`jito-labs/mev-protos`](https://github.com/jito-labs/mev-protos)
- Relayer reference (other side of the wire): [`jito-foundation/jito-relayer`](https://github.com/jito-foundation/jito-relayer)
