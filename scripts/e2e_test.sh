#!/usr/bin/env bash
# End-to-end smoke test for the Meridian block engine.
#
# Spins up a local solana-test-validator and a mock Jito upstream, runs the
# engine wired to both, connects a VALIDATOR-role bundle subscriber, blasts
# bundles with the searcher client, and asserts the validator receives both the
# Jito passthrough bundles and the bot bundles.
#
# Requires: solana CLI (solana-test-validator, solana-keygen) on PATH.
# Usage: ./scripts/e2e_test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LEDGER=$(mktemp -d)
SEARCHER_KP="${HOME}/.config/solana/id.json"
VAL_KP=$(mktemp -u).json
ENGINE_LOG=$(mktemp)
VALSUB_LOG=$(mktemp)
MOCK_JITO_LOG=$(mktemp)
PIDS=()

cleanup() {
  echo "--- cleaning up ---"
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  pkill -f "solana-test-validator --ledger $LEDGER" 2>/dev/null || true
  rm -rf "$LEDGER"
}
trap cleanup EXIT

echo "--- building ---"
cargo build -p jito-block-engine -p jito-searcher-client >/dev/null 2>&1

echo "--- starting test validator ---"
solana-test-validator --ledger "$LEDGER" --reset >/dev/null 2>&1 &
PIDS+=($!)
for _ in $(seq 1 40); do
  if curl -s -m 2 -X POST http://127.0.0.1:8899 -H 'Content-Type: application/json' \
       -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' | grep -q '"ok"'; then
    echo "validator healthy"; break
  fi
  sleep 2
done

[ -f "$SEARCHER_KP" ] || solana-keygen new --no-bip39-passphrase --silent --outfile "$SEARCHER_KP"
solana-keygen new --no-bip39-passphrase --force --silent --outfile "$VAL_KP"
SEARCHER=$(solana-keygen pubkey "$SEARCHER_KP")
VAL=$(solana-keygen pubkey "$VAL_KP")

echo "--- starting mock jito upstream ---"
RUST_LOG=info ./target/debug/mock_jito --addr 127.0.0.1:11010 --bundle-interval-ms 1000 \
  >"$MOCK_JITO_LOG" 2>&1 &
PIDS+=($!)
sleep 2

echo "--- starting engine ---"
RUST_LOG=info AUTH_JWT_SECRET=dev ALLOWED_PUBKEYS="$SEARCHER,$VAL" \
  ./target/debug/jito-block-engine \
  --tip-accounts "$SEARCHER" \
  --sim-rpc-url http://localhost:8899 \
  --tracker-rpc-url http://localhost:8899 \
  --auction-interval-ms 500 \
  --metrics-addr 127.0.0.1:9911 \
  --jito-block-engine-url http://127.0.0.1:11010 \
  --jito-block-engine-tls false \
  --identity-keypair "$VAL_KP" \
  >"$ENGINE_LOG" 2>&1 &
PIDS+=($!)
sleep 3

echo "--- starting validator subscriber ---"
RUST_LOG=info ./target/debug/validator_sub \
  --keypair-path "$VAL_KP" \
  --auth-service-url http://localhost:1005 \
  --validator-service-url http://localhost:1003 \
  >"$VALSUB_LOG" 2>&1 &
PIDS+=($!)
sleep 3

# The searcher warms up (~3-10s) before sending: airdrop + a fixed 5s settle,
# slower right after a cold validator start. Give it a generous window so the
# actual bundle blasting isn't cut off.
echo "--- blasting bundles (35s, incl. searcher warm-up) ---"
RUST_LOG=info timeout 35 ./target/debug/jito-searcher-client \
  --keypair-path "$SEARCHER_KP" \
  --auth-service-url http://localhost:1005 \
  --searcher-service-url http://localhost:1234 \
  --rpc-url http://localhost:8899 >/dev/null 2>&1 || true

echo "--- results ---"
WON=$(grep -c "auction:" "$ENGINE_LOG" || true)
RECEIVED=$(grep -c "RECEIVED bundle" "$VALSUB_LOG" || true)
JITO_PASSTHROUGH=$(grep -c "uuid=jito-" "$VALSUB_LOG" || true)
BOT_BUNDLES=$((RECEIVED - JITO_PASSTHROUGH))
echo "auction rounds with winners: $WON"
echo "bundles received by validator: $RECEIVED"
echo "  jito passthrough: $JITO_PASSTHROUGH"
echo "  bot bundles: $BOT_BUNDLES"
curl -s -m 3 http://127.0.0.1:9911/metrics | grep -E "bundles_won|tips_lamports|auth_success|jito_bundles_relayed|upstream_connects" || true

if [ "$JITO_PASSTHROUGH" -gt 0 ] && [ "$BOT_BUNDLES" -gt 0 ]; then
  echo "E2E PASS: jito passthrough and bot bundles both reached the validator"
else
  echo "E2E FAIL: jito=$JITO_PASSTHROUGH bot=$BOT_BUNDLES"; exit 1
fi
