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
| `relayer` | `BlockEngineRelayer` service — ingests packets; streams derived AOI/POI | ✅ **new** (reference never built this) |
| `interest` | derives accounts/programs of interest from submitted bundles | ✅ new |
| `validator` | `BlockEngineValidator` service — routes packets+bundles to the leading validator | ✅ leader-aware |
| `leader_tracker` | polls RPC for the leader schedule; answers "is X leading soon?" | ✅ new |
| `searcher` | `SearcherService` — accepts bundles; streams bundle results | ✅ `send_bundle` + `SubscribeBundleResults`; some RPCs `unimplemented!()` |
| `results` | routes per-bundle outcomes back to the submitting searcher | ✅ new |
| `tracker` | polls RPC for forwarded bundles; emits Processed/Finalized/Dropped | ✅ new (`--tracker-rpc-url`) |
| `auction` | scores bundles by tip, packs winners under a CU budget; accrues tip revenue | ✅ tip + real-CU/validity from simulation |
| `simulator` | RPC bundle simulation: per-tx or atomic `simulateBundle` | ✅ `--sim-rpc-url` (+ `--sim-atomic`) |
| `metrics` | process-wide counters; periodic log snapshot + Prometheus render | ✅ new |
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

## Implementation status (the parts Jito never open-sourced)

The full MEV pipeline is implemented and tested:

1. ~~**Real auth**~~ ✅ done — ed25519 challenge/response + HS256 JWT in
   `src/auth/`, enforced via an interceptor on the validator/relayer/searcher
   services, with a configurable pubkey allowlist (`--allowed-pubkeys`) and
   per-role scoping (a SEARCHER token can't subscribe on the validator service).
2. ~~**The auction**~~ ✅ done (step 4a) — bundles are buffered, scored by tip
   (lamports to `--tip-accounts`), and the highest tip-per-CU set that fits
   `--block-cu-limit` is emitted each `--auction-interval-ms` tick.
3. ~~**Bundle simulation**~~ ✅ done — `simulator` delegates to a Solana RPC
   (`--sim-rpc-url`, ideally your jito-solana validator): real CU replaces the
   estimate and bundles that fail simulation are dropped. Defaults to per-tx
   `simulateTransaction`; `--sim-atomic` uses jito-solana's atomic
   `simulateBundle` (state-aware, accurate for dependent bundles).
4. ~~**Leader-aware routing**~~ ✅ done — `leader_tracker` polls RPC for the
   schedule; the validator service tags each subscription with the validator's
   authenticated identity and forwards only to upcoming leaders. Enable with
   `--leader-rpc-url`; without it, traffic fans out to all (local testing).
5. ~~**Accounts/Programs of Interest**~~ ✅ done — the `interest` registry
   derives writable accounts + invoked programs from submitted bundles, and the
   relayer service streams them (use `--forward-all-packets` for the old "*"
   behaviour).
6. ~~**Expiry handling**~~ ✅ done — each packet batch carries an engine-local
   deadline derived from the relayer's `expiry_ms`; the validator forwarder
   drops batches past their deadline instead of forwarding stale packets.

The block-builder fee info is now configurable too (`--block-builder-pubkey`,
`--block-builder-commission`) instead of an all-1s placeholder.

7. ~~**Observability & shutdown**~~ ✅ done — the `metrics` crate tracks
   bundles received/won/dropped, accrued tip lamports, packets
   received/forwarded/expired, and auth challenges/success/failures, logged
   every 30s and scrapable at `GET /metrics` (`--metrics-addr`, default
   `0.0.0.0:9900`). The engine drains all servers cleanly on SIGINT/SIGTERM.
8. ~~**Bundle results**~~ ✅ done — searchers stream `SubscribeBundleResults`;
   the auction reports accepted/rejected and the `tracker`
   (`--tracker-rpc-url`) reports on-chain Processed/Finalized/Dropped.
9. ~~**Region discovery**~~ ✅ done — `GetBlockEngineEndpoints` advertises the
   configured `--block-engine-url` + `--regioned-endpoint`s.
10. ~~**Tip accounting**~~ ✅ done — won-bundle tips accrue to the
    `tips_lamports_total` metric.

**Genuinely out of scope** (a separate on-chain component, not engine code): the
TipPaymentProgram / TipDistributionProgram that perform the *on-chain* merkle
payout of accrued tips to validators and stakers. The engine does the off-chain
accounting and tells validators which pubkey collects the fee
(`GetBlockBuilderFeeInfo`); deploying/operating those Anchor programs is a
separate project.

## Testing end-to-end

`scripts/e2e_test.sh` runs the whole pipeline against a local
`solana-test-validator`: it starts the validator, runs the engine (auth +
auction + per-tx simulation + on-chain tracking), connects a VALIDATOR-role
bundle subscriber (`validator_sub`), blasts bundles with the searcher client,
and asserts they traverse auth → auction → simulate → forward → subscriber.

```bash
./scripts/e2e_test.sh
# ... E2E PASS: bundles flowed auth -> auction -> forward -> validator subscriber
```

The two test binaries it uses (not in the default build):

```bash
cargo build -p jito-searcher-client      # builds jito-searcher-client + validator_sub
```

- `jito-searcher-client` — authenticates as SEARCHER, airdrops, blasts bundles.
- `validator_sub` — authenticates as VALIDATOR, subscribes to the bundle stream.

## Provenance

- Skeleton: [`jito-labs/block_engine_simple`](https://github.com/jito-labs/block_engine_simple) (Apache-2.0)
- Protocol: [`jito-labs/mev-protos`](https://github.com/jito-labs/mev-protos)
- Relayer reference (other side of the wire): [`jito-foundation/jito-relayer`](https://github.com/jito-foundation/jito-relayer)
