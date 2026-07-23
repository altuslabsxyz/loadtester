---
name: loadtest-send
description: Use when driving/sending load at a stable chain with the loadtester - running `loadtester start`, choosing one-shot vs continuous, picking workload kinds and account count, sending value/vip/unordered/erc20/swap transactions, or watching a run in progress.
---

# loadtest-send

Drive the actual transactions. Assumes the target.yaml is ready (`loadtest-setup`);
interpreting the result is `loadtest-verify`.

**References:** `../loadtest-stable/config.md` (workload kinds, commands),
`../loadtest-stable/reference.md` (how the driver behaves on stable).

## Run
```bash
loadtester start -t target.yaml -d deployment.json -o out --fail-on review
```
- `-d`: empty `{}` is fine for token-free workloads (`value`/`vip`/`unordered`).
- `--fail-on review|fail|none`: process exit code when overall meets the threshold (one-shot only).
- A first seeded run may float roughly `accountsN × fundPerAccount` whole tokens; later runs top up only deficient accounts. Confirm the preflighted amount before launching.

## How the load actually behaves on stable (so results make sense)
- **Bounded conflict-free scheduler**: at most `workload.workers` send attempts run concurrently and `workload.targetTPS` caps the aggregate dispatch rate. A full worker queue drops that opportunity instead of building stale backlog. An account with a tx in flight is NOT eligible to send (ownership state machine + per-block hash confirmation), so the engine cannot trigger the chain's "already in mempool" / "replacement rule" rejections; its only same-nonce resend is a deliberate ~1.3x fee-bumped replacement after a 90s TTL. Watch the 15s `[load] progress:` line — `pending` ≈ txs in the mempool from us, `ready` = accounts eligible, `starved` climbing = every account in flight (the chain's inclusion rate is the bottleneck, add accounts or accept the plateau).
- **1-in-flight per ordered account**: each account/nonce-key has an exclusive slot. Standard traffic sends only when pending nonce equals confirmed nonce; a still-pending account is skipped and a dropped nonce is safely reused.
- **Large pool, bounded concurrency**: `accountsN` controls how long before an address is revisited, not goroutine count. `50,000` accounts at `5,000` TPS gives an approximately 10-second rotation while `workers: 256` bounds RPC pressure.
- **Funding is balance-aware**: the doubling tree tops up only deficits. A fully funded seeded pool sends no top-up transactions.
- **`targetInflight` is a workload-mix weight**, not per-account depth or a rate. `targetTPS` is the global rate cap.
- **vip** goes only to the `role: vip` node and is skipped (logged) if there isn't one.
- Each value/vip/unordered tx sends **1 wei**; gas is paid from the account's funded balance.

## Watching a run
- Phase logs: `[setup]` → `[lanes]` → `[observe]` → `[load] running … for Ns` → `[load] done; N txs sent` → `[drain]` → `[report] final written`.
- Live mempool depth = CometBFT `num_unconfirmed_txs` on the submit node's `cometRPC` (the EVM `txpool_*` RPCs are always 0 on stable — don't watch those).
- Recommended: run the chain-side monitor (`../loadtest-stable/chain-monitor.md`) concurrently to catch chain problems independent of the load.

## Common mistakes
- Treating `txpool_status` as the mempool signal — it's vestigial (0) on stable; use CometRPC `num_unconfirmed_txs`.
- Cranking `targetInflight` expecting a higher total rate — set `targetTPS`; use `targetInflight` only to choose the relative lane mix.
- Setting `workers` equal to a 50k account pool — keep workers bounded (256 is the default) unless the RPC endpoint is proven to tolerate more.
- Killing a run during setup — funding is only ~`log2(accountsN)` blocks, but token prep (mint+approve) still takes a few blocks; tail the log before assuming a hang.
