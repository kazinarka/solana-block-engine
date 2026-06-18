# Meridian Block Engine — A Plain-English Overview

This document explains, in everyday language, what this project is and what it
does. No prior knowledge of the code is needed.

## The one-sentence version

We built a **block engine**: a service that collects "pay-to-be-included"
transaction requests from traders, checks they're real, picks the most valuable
batch, and hands them to whichever Solana validator is about to produce the next
block — so the trader's transactions land where they want and the validator
earns the tips.

## A quick analogy

Imagine Solana as a print shop that publishes a new page of a shared ledger
every fraction of a second. The shop has many printers (validators), and they
take turns being the one printing the next page (the "leader").

- **Traders** (called *searchers*) want their transactions printed in a precise
  order — for example, to win an arbitrage. They're willing to **tip** to get
  that placement.
- A **block engine** is the auction house that sits next to the print shop. It
  gathers the traders' tipped requests, makes sure each one actually works,
  chooses the best-paying combination that will fit on the page, and rushes them
  to whoever is printing next.
- A **relayer** is the mailroom: it receives the flood of incoming mail
  (transactions) and forwards only the relevant pieces to the auction house.

This project is the **auction house** — your own, independent one.

## The cast of characters

- **Validator** — a computer that helps run Solana and sometimes gets to build
  the next block. You already run one.
- **Leader** — the validator whose turn it is to build the block right now.
- **Searcher** — a trader who submits bundles and pays tips for good placement.
- **Bundle** — a small group of transactions that must run together, in order,
  all-or-nothing.
- **Tip** — a payment the searcher attaches to win the auction.
- **Relayer** — the mailroom that feeds transactions to the engine. You already
  run one.
- **Block engine** — the matchmaker/auctioneer. **This is what we built.**

## What the engine actually does

Think of it as a series of checkpoints every bundle passes through:

1. **Front door (security).** Anyone connecting — a trader, your relayer, your
   validator — must prove who they are first, using a digital signature
   challenge. They get a time-limited pass. Different kinds of visitors get
   different passes (a trader's pass can't be used to impersonate a validator).
   You can also keep a guest list so only approved participants get in.

2. **Taking in requests.** Traders submit their bundles. The engine notes which
   accounts each bundle touches and tells the relayer, "only send me the mail
   that's relevant to these," so the engine isn't drowned in everything.

3. **The auction.** Every so often (a few times a second) the engine looks at
   all the bundles waiting, reads how much each one tips, and picks the
   best-paying combination that will still fit in a block. The rest are set
   aside.

4. **Reality check (simulation).** Before committing, the engine asks a Solana
   node to dry-run each bundle: *Would this actually succeed? How much of the
   block's capacity would it use?* Bundles that would fail are thrown out so
   they don't waste space.

5. **Delivery to the right validator.** The engine knows the schedule of who
   builds blocks next. It sends the winning bundles only to the validator that's
   about to build — not to everyone.

6. **Following up.** After delivery, the engine watches the chain to see whether
   each bundle actually made it in, and tells the trader the outcome
   (landed / didn't land / was rejected).

7. **Keeping score.** Throughout, the engine tracks numbers — how many bundles
   came in, how many won, how much tip money was earned, how many failed — and
   makes them available for monitoring dashboards. It also shuts down cleanly
   when asked, so it's safe to run as a real service.

## What makes it *yours*

This is a **fully independent** block engine. It does **not** rely on Jito Labs'
servers, their approval, their auction, or any revenue sharing. You run it, you
set the rules, you keep the tips, you decide who's allowed in.

The one thing it deliberately reuses is the **"language"** that Solana
validators already speak for this purpose. Your validator software already knows
how to talk to a block engine; rather than reinventing that conversation, the
engine speaks the same language — so your existing validator and relayer can
point at *your* engine instead of someone else's. That's what "your own block
engine, outside Jito" means in practice: your own operation, using the standard
plug.

## How we know it works

We didn't just write the code — we ran it. On a private, throwaway copy of
Solana running on the same machine, we:

- started the engine,
- connected a stand-in trader and a stand-in validator,
- had the trader blast hundreds of bundles,
- and confirmed they flowed all the way through: logged in, auctioned,
  dry-run-checked, delivered to the validator, and accounted for.

The numbers lined up exactly — every bundle the engine said it picked was a
bundle the validator actually received, and the tip totals added up. This whole
test is saved as a script so it can be re-run any time.

We also have a set of automated checks (21 of them) that verify the tricky
individual parts — the login security, the auction math, the tip reading, the
result routing — every time the code changes.

## What's left before real money

Being honest about the line between "works in our tests" and "ready for
mainnet":

- **The real validator handshake.** We tested with our own stand-in validator.
  Before production, your *actual* validator software should connect to the
  engine (on a test network first) to confirm the real conversation works
  end-to-end.
- **Encryption (TLS).** Right now the connections are unencrypted, which is fine
  for a local test but must be turned on before exposing the engine to a
  network.
- **The on-chain payout program.** The engine tracks how much tip money was
  earned and tells the validator which account collects it, but the final
  on-chain distribution of those tips to the validator and its stakers is a
  separate on-chain program — not part of the engine itself.
- **Real-world load.** It's been tested with light traffic, not the volume and
  split-second timing of live mainnet.

The recommended path is: turn on encryption, then point your own validator at it
on a test network, then graduate to mainnet.

## Where things live

- The code: `src/` (each folder is one focused piece — security, auction,
  simulation, delivery, monitoring, and so on).
- A technical summary for engineers: `README.md`.
- The end-to-end test you can re-run: `scripts/e2e_test.sh`.
