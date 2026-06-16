#!/usr/bin/env bash
# End-to-end smoke test for the Meridian block engine.
#
# Spins up a local solana-test-validator, runs the engine wired to it
# (auth + auction + per-tx simulation + on-chain tracking), connects a
# VALIDATOR-role bundle subscriber, blasts bundles with the searcher client, and
# asserts the bundles traverse auth -> auction -> simulate -> forward -> subscriber.
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

echo "--- starting engine ---"
RUST_LOG=info AUTH_JWT_SECRET=dev ALLOWED_PUBKEYS="$SEARCHER,$VAL" \
  ./target/debug/jito-block-engine \
  --tip-accounts "$SEARCHER" \
  --sim-rpc-url http://localhost:8899 \
  --tracker-rpc-url http://localhost:8899 \
  --auction-interval-ms 500 \
  --metrics-addr 127.0.0.1:9911 \
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

echo "--- blasting bundles (15s) ---"
RUST_LOG=info timeout 15 ./target/debug/jito-searcher-client \
  --keypair-path "$SEARCHER_KP" \
  --auth-service-url http://localhost:1005 \
  --searcher-service-url http://localhost:1234 \
  --rpc-url http://localhost:8899 >/dev/null 2>&1 || true

echo "--- results ---"
WON=$(grep -c "auction:" "$ENGINE_LOG" || true)
RECEIVED=$(grep -c "RECEIVED bundle" "$VALSUB_LOG" || true)
echo "auction rounds with winners: $WON"
echo "bundles received by validator: $RECEIVED"
curl -s -m 3 http://127.0.0.1:9911/metrics | grep -E "bundles_won|tips_lamports|auth_success" || true

if [ "$RECEIVED" -gt 0 ]; then
  echo "E2E PASS: bundles flowed auth -> auction -> forward -> validator subscriber"
else
  echo "E2E FAIL: validator received no bundles"; exit 1
fi
