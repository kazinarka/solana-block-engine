# Vendored protocol definitions

These `.proto` files are vendored verbatim from Jito's public MEV protocol so the
engine speaks the same gRPC dialect as a jito-solana validator / relayer.

- **Source:** https://github.com/jito-labs/mev-protos
- **Synced to commit:** `46ead86a13a55a0ef2c139db96a8ee93bf7505e3`
- **Upstream date:** 2025-07-23 ("Add validator endpoint discovery")
- **Status:** all files byte-identical to upstream `master` as of this sync.

## Refreshing

```bash
git clone --depth 1 https://github.com/jito-labs/mev-protos.git /tmp/mev-protos
cp /tmp/mev-protos/*.proto src/jito_protos/protos/
# then rebuild; if services fail to compile, the protocol changed and the
# service impls in src/{validator,searcher,relayer,auth}/ must be updated to
# match the new trait surface.
```

Update the commit hash above whenever you re-vendor.
