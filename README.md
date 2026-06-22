# Meridian Block Engine

A self-hosted Solana block engine. It receives MEV **bundles** from searchers,
runs a tip-based auction (with on-chain simulation), and streams the winning
bundles to the validator that is about to produce a block — while feeding a
relayer the accounts/programs it should forward and reporting each bundle's fate
back to its submitter.

It speaks the public [Jito MEV protocol](https://github.com/jito-labs/mev-protos)
(vendored under `src/jito_protos/protos/`), so an unmodified jito-solana
validator and a jito-style relayer can connect to it without modification. It is
independent of Jito Labs' infrastructure: you run the engine, set the auction
rules, configure the tip accounts, and keep the tips.

> **Validator requirement:** bundles only reach a block if the leader runs a
> validator client that ingests from an external block engine — today that means
> **jito-solana** (stock Agave has no such hook). Pointing a jito-solana
> validator's `--block-engine-url` at this engine is the supported deployment.

## Architecture

```
 searchers ──SendBundle──────────────────┐
                                          ▼
 relayer ──StartExpiringPacketStream──►  BLOCK ENGINE  ──packets+bundles──► validator (jito-solana)
   ▲                                       │  auth (ed25519 + JWT, per role)
   └──SubscribeAccountsOfInterest──────────┤  auction (tip scoring + CU packing)
                                           │  simulation (RPC dry-run)
 searchers ◄──SubscribeBundleResults───────┘  leader-targeted routing + result tracking
```

The engine runs four gRPC services plus an HTTP metrics endpoint:

| Service | Default port | Who connects | Purpose |
|---------|--------------|--------------|---------|
| `AuthService` | `1005` | everyone (first) | ed25519 challenge/response → JWT access/refresh tokens |
| `SearcherService` | `1234` | searchers | submit bundles, stream bundle results |
| `BlockEngineRelayer` | `1004` | the relayer | stream packets in, receive accounts/programs of interest |
| `BlockEngineValidator` | `1003` | the validator | subscribe to packets + winning bundles |
| metrics (HTTP) | `9900` | Prometheus | `GET /metrics` |

## How it works (request logic)

### 1. Authentication

Every connection authenticates before using a service. The flow matches the
canonical jito-relayer client:

1. Client calls `GenerateAuthChallenge { role, pubkey }`; the server returns a
   random challenge nonce (stored per-pubkey with a short TTL).
2. Client signs the string `"{pubkey_base58}-{challenge}"` with its ed25519 key
   and calls `GenerateAuthTokens { challenge, client_pubkey, signed_challenge }`.
3. The server rebuilds the expected string, verifies the signature against the
   pubkey, and issues an **access** + **refresh** JWT (HS256, signed with an
   in-process secret).

The `AuthInterceptor` enforces a valid `Bearer` token on every other service and
is **role-scoped**: a `SEARCHER` token is rejected on the validator service, and
so on. An optional allowlist (`--allowed-pubkeys`) restricts who may authenticate
at all. The auth state (challenge store + signing key) is shared in-process, so
the issuing service and the validating interceptors agree on the same secret.

### 2. Packet intake (relayer → engine)

The relayer opens a bidirectional `StartExpiringPacketStream` and pushes
`ExpiringPacketBatch` messages. Each batch carries an `expiry_ms` (the relayer's
hold window); the engine converts that into a local deadline and forwards the
batch into the validator-forwarder channel. Batches past their deadline are
dropped rather than forwarded, so stale packets never reach the leader.

In return, the engine streams **accounts/programs of interest** to the relayer
(`SubscribeAccountsOfInterest` / `SubscribeProgramsOfInterest`). These are
derived from submitted bundles: when a searcher submits a bundle, the engine
records every *writable* account it references and every program it invokes
(with a TTL). The relayer then forwards only transactions touching that contended
state — not the entire packet flow. Setting `--forward-all-packets` advertises
`"*"` instead (forward everything), which is useful for local testing.

### 3. Bundle submission and the auction

Searchers call `SendBundle`. Each bundle is recorded against its owner (for
result routing), scanned for interest, and pushed into the auction buffer with a
score:

- **Tip** — the sum of lamports transferred via SystemProgram to any configured
  `--tip-accounts` across the bundle's transactions.
- **Compute units** — taken from simulation if available, otherwise a flat
  `EST_CU_PER_TX` estimate per transaction.

On every `--auction-interval-ms` tick the engine:

1. Optionally simulates any not-yet-simulated bundles (see below), recording real
   CU and dropping ones that fail.
2. Drops bundles older than `--bundle-ttl-ms`.
3. Ranks the rest by **tip-per-CU** (value density) and greedily selects the set
   that fits `--block-cu-limit` — a knapsack heuristic that maximises total tip
   within the block's compute budget.
4. Emits the winners; the remainder are reported as auction losers and dropped.

### 4. Simulation

When `--sim-rpc-url` is set, bundles are dry-run against a Solana RPC node before
they can win:

- **Per-transaction** (default): each transaction is simulated with
  `simulateTransaction` and `replace_recent_blockhash`; CU is summed and the
  bundle fails on the first error. Works against any RPC.
- **Atomic** (`--sim-atomic`): the whole bundle is simulated via jito-solana's
  `simulateBundle` JSON-RPC, which runs the transactions sequentially against
  shared state — accurate for bundles whose later transactions depend on earlier
  ones. Requires a jito-solana RPC.

Failing bundles are dropped so they don't waste block space. Without a sim RPC,
the engine uses the CU estimate and never drops for failure.

### 5. Leader-targeted delivery

Validators subscribe to `SubscribePackets` and `SubscribeBundles`. Each
subscription is tagged with the validator's authenticated identity (the JWT
`sub`). When `--leader-rpc-url` is set, a background tracker polls
`getLeaderSchedule` / `getEpochInfo` and the forwarder delivers only to the
validator(s) leading within `--leader-lookahead-slots` of the current slot.
Without a leader RPC, traffic fans out to all connected validators.

### 6. Result tracking (engine → searcher)

Searchers call `SubscribeBundleResults` to receive their bundles' outcomes,
routed only to the submitting searcher:

- `Accepted` — won the auction and was forwarded.
- `Rejected` — lost the auction or failed simulation.
- `Processed` / `Finalized` / `Dropped` — emitted by the on-chain tracker
  (`--tracker-rpc-url`), which extracts each forwarded bundle's transaction
  signatures and polls `getSignatureStatuses` until they confirm, finalize, or
  exceed `--tracker-deadline-secs` without landing.

### 7. Observability and lifecycle

Process-wide counters (bundles received/won/dropped, accrued tip lamports,
packets received/forwarded/expired, auth challenges/success/failures, validator
subscriptions) are logged every 30s and exposed at `GET /metrics`
(`--metrics-addr`) in Prometheus format. The engine drains all servers on
`SIGINT`/`SIGTERM` for clean restarts.

## Crate layout (`src/`)

| Crate | Role |
|-------|------|
| `jito_protos` | Generated gRPC bindings from the vendored MEV protocol |
| `auth` | `AuthService`, JWT issue/verify, the role-scoped interceptor |
| `searcher` | `SearcherService` — bundle submission + result subscription |
| `relayer` | `BlockEngineRelayer` — packet intake + interest advertisement |
| `validator` | `BlockEngineValidator` — packet/bundle fan-out, leader routing |
| `auction` | Tip scoring and CU-budget winner selection |
| `simulator` | RPC bundle simulation (per-tx or atomic) |
| `interest` | Derives accounts/programs of interest from bundles |
| `leader_tracker` | Tracks the leader schedule via RPC |
| `tracker` | Tracks forwarded bundles on-chain, emits results |
| `results` | Routes per-bundle outcomes to the submitting searcher |
| `metrics` | Process-wide counters + Prometheus rendering |
| `block_engine` | Binary that wires the services together |
| `searcher_client` | Test tools: a bundle blaster + a `validator_sub` subscriber |

## Configuration

All options are CLI flags or environment variables.

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--searcher-addr` | `SEARCHER_ADDR` | `0.0.0.0:1234` | searcher service bind |
| `--validator-addr` | `VALIDATOR_ADDR` | `0.0.0.0:1003` | validator service bind |
| `--relayer-addr` | `RELAYER_ADDR` | `0.0.0.0:1004` | relayer service bind |
| `--auth-addr` | `AUTH_ADDR` | `0.0.0.0:1005` | auth service bind |
| `--metrics-addr` | `METRICS_ADDR` | `0.0.0.0:9900` | Prometheus endpoint bind |
| `--auth-jwt-secret` | `AUTH_JWT_SECRET` | random | HS256 token secret (set it to survive restarts) |
| `--allowed-pubkeys` | `ALLOWED_PUBKEYS` | empty (any) | comma-separated base58 allowlist |
| `--tip-accounts` | `TIP_ACCOUNTS` | empty | comma-separated base58 tip accounts |
| `--auction-interval-ms` | — | `200` | auction tick period |
| `--block-cu-limit` | — | `48000000` | per-block CU budget |
| `--bundle-ttl-ms` | — | `200` | drop bundles older than this |
| `--sim-rpc-url` | `SIM_RPC_URL` | unset | RPC for bundle simulation |
| `--sim-atomic` | `SIM_ATOMIC` | false | use jito-solana `simulateBundle` |
| `--leader-rpc-url` | `LEADER_RPC_URL` | unset | RPC for leader schedule (else fan out to all) |
| `--leader-lookahead-slots` | — | `2` | how far ahead a validator counts as leader |
| `--tracker-rpc-url` | `TRACKER_RPC_URL` | unset | RPC for on-chain result tracking |
| `--tracker-deadline-secs` | — | `90` | mark a bundle dropped if not landed by then |
| `--forward-all-packets` | `FORWARD_ALL_PACKETS` | false | advertise `"*"` to the relayer |
| `--interest-ttl-ms` | — | `2000` | how long an account stays "of interest" |
| `--block-builder-pubkey` | — | system pubkey | fee collector returned to validators |
| `--block-builder-commission` | — | `5` | block-builder commission percent |
| `--block-engine-url` | `BLOCK_ENGINE_URL` | unset | global endpoint advertised for region discovery |
| `--shredstream-addr` | `SHREDSTREAM_ADDR` | empty | shredstream addr for the global endpoint |
| `--regioned-endpoint` | `REGIONED_ENDPOINT` | empty | `url\|shredstream` regioned endpoints (comma-separated) |

## Build & run

Built against the Solana/Agave **3.x** crate line; requires a recent stable Rust
(MSRV ≥ 1.89 — `rustup update stable`).

```bash
cargo build --release
RUST_LOG=info ./target/release/jito-block-engine \
  --allowed-pubkeys <validator>,<relayer>,<searcher pubkeys> \
  --tip-accounts <your tip account(s)> \
  --leader-rpc-url <your validator RPC> \
  --sim-rpc-url <your jito-solana RPC> --sim-atomic \
  --tracker-rpc-url <your RPC>
```

Then point a jito-solana validator's `--block-engine-url` at `:1003`, your
relayer at `:1004` (both authenticate via `:1005`), and scrape `:9900/metrics`.

The default build excludes the heavy Solana client used by the test tools; build
them on demand:

```bash
cargo build -p jito-searcher-client   # builds jito-searcher-client + validator_sub
```

## Proxy mode (interpose on Jito)

The engine can run in front of Jito's block engine: it authenticates upstream to
Jito as your validator, relays Jito's bundles, packets, and fee info to your
validator unchanged, and additively merges your own bundles (submitted by your
MEV bot through the searcher service). Your validator then points at this proxy
instead of at Jito directly.

```bash
RUST_LOG=info ./target/release/jito-block-engine \
  --jito-block-engine-url https://<region>.block-engine.jito.wtf \
  --identity-keypair /path/to/validator-identity.json \
  --allowed-pubkeys <your bot searcher pubkey>,<your validator pubkey> \
  --leader-rpc-url <your RPC> --sim-rpc-url <your RPC> --tracker-rpc-url <your RPC>
```

Jito bundles pass through untouched; the bot's bundles are appended. If the
upstream link drops, the proxy keeps serving the validator and reconnects with
backoff — it never blocks block production. To roll back, repoint the validator's
`--block-engine-url` at Jito and restart.

Deployment notes:

- Run the proxy on the validator host so the identity key never leaves the box.
- Validate on testnet first, then mainnet with the rollback path ready.
- **Before connecting a real jito-solana validator**: it authenticates and
  subscribes over a single `--block-engine-url`, so the proxy must serve the
  `AuthService` and `BlockEngineValidator` on one shared address. This build
  still exposes them on separate ports (`:1005` / `:1003`), which the bundled
  test clients use; co-locating them on one endpoint is the remaining step for
  real-validator hookup.

## Testing

```bash
cargo test            # unit + integration tests across the workspace
./scripts/e2e_test.sh # full pipeline against a local solana-test-validator
```

The e2e script starts a test validator, runs the engine, connects a
VALIDATOR-role subscriber, blasts bundles with the searcher client, and asserts
they flow auth → auction → simulation → forward → subscriber.

## Tech stack

- **Rust**, `tonic` (gRPC) + `prost` (protobuf), `tokio` async runtime.
- `axum` for the metrics HTTP endpoint.
- `jsonwebtoken` (HS256) + `ed25519-dalek` for auth.
- `solana-client` / `solana-sdk` (3.x) for RPC and transaction types.

## Scope

The engine performs the off-chain MEV pipeline and the auction. It does not
include the on-chain TipPayment / TipDistribution programs that perform the
on-chain payout of accrued tips to validators and stakers — those are separate
on-chain components. The engine tracks accrued tips and reports the fee collector
pubkey via `GetBlockBuilderFeeInfo`.

## Provenance

- Skeleton: [`jito-labs/block_engine_simple`](https://github.com/jito-labs/block_engine_simple) (Apache-2.0)
- Protocol: [`jito-labs/mev-protos`](https://github.com/jito-labs/mev-protos) (see `src/jito_protos/protos/VENDOR.md`)
- Relayer reference: [`jito-foundation/jito-relayer`](https://github.com/jito-foundation/jito-relayer)
