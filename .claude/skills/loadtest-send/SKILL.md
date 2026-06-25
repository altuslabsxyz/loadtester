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
- A real-funds run spends up to `accountsN × fundPerAccount` whole tokens — confirm that's intended before launching. For long testnet runs, launch in the background and watch the log.

## How the load actually behaves on stable (so results make sense)
- **1-in-flight per account, closed-loop**: each account sends one tx, waits for its receipt, then the next. Throughput scales with `accountsN`, NOT per-account inflight (the chain rejects future nonces).
- **Funding is lockstep** → setup takes ~`accountsN` blocks before load starts. Don't mistake a slow setup for a hang; tail the log (`[setup] accounts funded`).
- **unordered** is rate-limited fire-and-forget (~`targetInflight` tx/s); it won't flood/starve the ordered lanes.
- **vip** goes only to the `role: vip` node and is skipped (logged) if there isn't one.
- Each value/vip/unordered tx sends **1 wei**; gas is paid from the account's funded balance.

## Watching a run
- Phase logs: `[setup]` → `[lanes]` → `[observe]` → `[load] running … for Ns` → `[load] done; N txs sent` → `[drain]` → `[report] final written`.
- Live mempool depth = CometBFT `num_unconfirmed_txs` on the submit node's `cometRPC` (the EVM `txpool_*` RPCs are always 0 on stable — don't watch those).
- Recommended: run the chain-side monitor (`../loadtest-stable/chain-monitor.md`) concurrently to catch chain problems independent of the load.

## Common mistakes
- Treating `txpool_status` as the mempool signal — it's vestigial (0) on stable; use CometRPC `num_unconfirmed_txs`.
- Cranking `targetInflight` expecting more ordered load — raise `accountsN` instead.
- Killing a run that looks stuck during lockstep funding — it's just slow, not hung.
