# Ligate fork of Sovereign SDK

This is `ligate-io/sovereign-sdk`, a fork of `Sovereign-Labs/sovereign-sdk`
that carries patches needed by Ligate Chain that aren't (yet) upstream.
The chain consumes this fork via a `[patch."https://github.com/Sovereign-Labs/sovereign-sdk.git"]`
block in its workspace `Cargo.toml`, pinned to a specific commit on
`ligate-mainline`.

This file documents the workflow. The upstream Sovereign Labs
[`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) still applies for
everything we do upstream.

## Branches

| Branch | Owner | Purpose |
|---|---|---|
| `dev` | Sovereign Labs (read-only mirror via `upstream` remote) | Their default branch. We rebase on top. |
| `ligate-mainline` | us | Default branch of this fork. Carries our patches on top of `upstream/dev`. **The chain pins commits on this branch.** |
| `feat/<topic>` | us | Per-feature branches. Used for upstream PRs to Sovereign Labs (one feature per PR keeps the diff reviewable). |

## What's currently on `ligate-mainline`

- `Sequencer::pending_tx_count()` for operator metrics. Open as upstream PR `Sovereign-Labs/sovereign-sdk#2838`. **Drop on rebase when that PR merges.**
- Typed `BlobSubmissionError` variant on `BlobSubmissionStatus`. Open as `Sovereign-Labs/sovereign-sdk#2839`. **Drop on rebase when that merges.**
- Bech32m hash types in `sov-rollup-interface`. Six concrete newtypes,
  one per HRP, plus matching `pub type` aliases:

  | Type alias | Newtype | HRP | What it identifies |
  |---|---|---|---|
  | `TxHash` | `LtxHash` | `ltx` | Rollup transaction hash |
  | `BlockHash` | `LblkHash` | `lblk` | DA / slot block hash |
  | `StateRootHash` | `LsrHash` | `lsr` | Rollup state root |
  | `BatchHash` | `LbaHash` | `lba` | Sequencer batch hash |
  | `ChainHash` | `LschHash` | `lsch` | Wallet schema commitment (`Runtime::CHAIN_HASH`) |
  | `BlobHash` | `LbzHash` | `lbz` | DA blob hash |

  Wired through `sov-ledger-apis` (`Slot`, `Batch`, `Transaction`),
  `sov-rollup-apis` schema endpoint, `sov-blob-sender`,
  `sov-db::ledger_db`, `sov-blob-storage` capabilities, plus the
  celestia + mock-da adapters (`TmHash` / `MockHash` Display + FromStr,
  `TmHashSchema` wallet attribute, `BlobWithSender.hash`).
  `SubmitBlobReceipt.blob_hash` and `DiscardedBlob.hash` are typed as
  `BlobHash`. `SchemaResponse.chain_hash` is typed as `ChainHash`.
  **Permanent unless we land a `HashEncoding` hook upstream.** Tracking
  issue: Sovereign-Labs/sovereign-sdk#2837.
- `NumberOrHash::Hash(TxHash)` in `sov-ledger-apis` so URL paths accept
  `ltx1...` (depends on the bech32m types). Permanent for the duration
  of the bech32m fork.

## When upstream advances `dev`

1. `git fetch upstream`
2. `git checkout ligate-mainline`
3. `git rebase upstream/dev`
4. Resolve any conflicts. Common spots:
   - `BlobSubmissionStatus` match arms in `sov-blob-sender/src/lib.rs` if Sovereign adds a new variant
   - `TxHash`, `BlockHash`, `BatchHash`, `BlobHash` consumers if Sovereign refactors auth / capabilities / DA adapters
   - `HexString` / `HexHash` callsites that we re-typed to one of the bech32m aliases
   - `MockHash` / `TmHash` `Display` / `FromStr` if upstream rewrites the adapter type modules
   - `TmHashSchema` `#[sov_wallet]` attribute if upstream changes wallet-derive parsing
5. `cargo check --workspace --exclude risc0 --exclude sp1 --exclude sov-eip712-auth` (set `LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib`, `PROTOC=$(which protoc)`, `SKIP_GUEST_BUILD=1` on macOS; `sov-eip712-auth` has a pre-existing upstream feature-unification break that's unrelated to our patches)
6. `cargo test -p sov-rollup-interface --lib bech32m` (all six HRP round-trips should pass)
7. `git push --force-with-lease origin ligate-mainline`
8. Bump the `rev` in `ligate-chain/Cargo.toml`'s `[patch.<sov-url>]` block to the new tip
9. Boot the chain's localnet smoke: submit a transfer, confirm `ltx1...` tx hashes still flow, slot lookups still surface `lblk1...` / `lsr1...`

## When you write a new patch

Two routes, depending on whether you want to upstream it:

**A. Upstreamable** (most patches should go this way):
1. Create `feat/<topic>` branch off `upstream/dev`
2. Implement the patch
3. Open PR to `Sovereign-Labs/sovereign-sdk:dev`
4. **Also cherry-pick the commit onto `ligate-mainline`** so the chain can consume it now without waiting for upstream review
5. When upstream merges, drop the cherry-picked commit during the next rebase

**B. Ligate-specific** (patches Sovereign explicitly won't take):
1. Branch off `ligate-mainline`
2. Implement
3. Merge into `ligate-mainline` (no upstream PR)
4. Live with the rebase cost forever

## Updating the chain's pin

The chain pins to a specific commit on `ligate-mainline` for reproducibility. To bump:

1. Push the new `ligate-mainline` tip (e.g. after a rebase)
2. In `ligate-chain/Cargo.toml`, find the `[patch."https://github.com/Sovereign-Labs/sovereign-sdk.git"]` block
3. Replace every `rev = "<old>"` with `rev = "<new full hash>"` (use the full 40-char hash; abbreviated hashes don't fetch reliably)
4. `cargo check --workspace` from the chain repo
5. PR + merge

Auto-bumping (tracking the branch HEAD without explicit revs) is **deliberately not done** — it would let upstream changes land in the chain unreviewed.

## Testing

The fork's tests run in two regimes:

| What | When | Command |
|---|---|---|
| Upstream test suite | Every PR on the fork (Sovereign's own CI configured here too) | `make test` |
| Bech32m hash unit tests | Every PR on the fork | `cargo test -p sov-rollup-interface --lib bech32m` |
| Chain localnet smoke | After bumping the chain pin | Boot `cargo run --bin ligate-node` from `ligate-chain`, submit a transfer, confirm `ltx1...` hashes round-trip |

## Why fork instead of upstream?

The bech32m hash encoding is the only patch we expect to maintain long-term. The other two (`pending_tx_count`, `BlobSubmissionError`) are open upstream PRs that we expect to merge. We fork because:

1. Sovereign's `sov_universal_wallet(display(bech32m(prefix = ...)))` derive takes a `&'static str`-returning function as the prefix, not a generic HRP marker — so a generic `Bech32mEncoded<T, H: Hrp>` can't reuse the existing macro infrastructure. Concrete-per-HRP types (`LtxHash`, `LblkHash`, `LsrHash`) sidestep this but aren't generalizable enough to be a clean upstream contribution.
2. Different Sovereign-SDK chains might want different encodings; we don't want to dictate ours upstream.
3. Sovereign would likely not accept the per-type approach because it's chain-specific.

We've filed `Sovereign-Labs/sovereign-sdk#2837` proposing an extensible `HashEncoding` trait that would let chains opt in without forking. If they accept it, we drop the fork.
