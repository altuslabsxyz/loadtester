---
name: loadtest-setup
description: Use when preparing or configuring a stable-chain load test - writing/editing the target.yaml, choosing nodes and workload kinds, capping how much the funding account may spend, building the loadtester binary, or preflighting a config before a run.
---

# loadtest-setup

Configure and preflight a stable-chain load test. This is step 1; sending the
load is `loadtest-send`, interpreting results is `loadtest-verify`.

**Authoritative references (read these for detail, don't duplicate):**
- `../loadtest-stable/config.md` — full `target.yaml` schema, workload kinds, spend-cap math, examples.
- `../loadtest-stable/reference.md` — chain facts behind the settings.

## Steps
1. **Build**: `make install` (binary on PATH) or `make build` (`./bin/loadtester`).
2. **Write the target.yaml** (see config.md schema). Decide:
   - `nodes`: one `jsonrpc` is required. Add `cometRPC` to make Goal 2/3 + lane quotas observable. Add a `role: vip` node only if you want VIP txs. Leave `grpc: ""` on a TLS public testnet (the tool dials gRPC insecure-only).
   - `governance.mode`: **preconfigured** for a testnet (lanes already registered; declare them in `blockspace.lanes` if you want lane verification). fast-pass/real-vote only on a local chain you control.
   - `workload.lanes`: token-free = `value`/`vip`/`unordered` (empty `deployment.json {}`). erc20/swap need a deployed mintable token. Keep `allowDestructive: false`.
   - `workload.durationSec`: **> 0** to get a verdict; `<= 0` is continuous (no verdict).
   - For reusable high-rate load, set a secret `funding.accountSeed`, an address-only `funding.accountsFile`, a large `accountsN`, bounded `workload.workers`, and aggregate `workload.targetTPS`. Never commit the seed; the generated manifest is safe to commit.
3. **Cap spend** (critical on a real funded account): on the first run the master needs roughly `accountsN × fundPerAccount` plus funding gas **available UPFRONT to float**. Funding reads every derived account balance and tops up only deficits, so later seeded runs need only replacement funds. `fundPerAccount` is DECIMAL whole tokens (`"0.01"`, `"1"`, …; fund only what gas needs, ~0.00002/tx). The exact master-balance precheck aborts before sending if the balance is insufficient. Ephemeral pools sweep by default; seeded pools retain balances by default for reuse. Set `sweepBack` explicitly only when overriding that behavior.
4. **Preflight**: `loadtester config -t target.yaml` — verify chainId, the load + vip endpoints, lane source, `mode`, and that the masked masterKey's derived address is the funded one. Abort on anything wrong.

## Sizing for a tx-per-block target (e.g. "10k txs in a block")
The chain admits ONE ordered tx per (sender, nonce-key) in the mempool — a block can never
contain more of your ordered txs than accounts pending at proposal. Size from that identity:
- **txs/block ≈ min(targetTPS × block_interval, accountsN × utilization, mempool.size, block max_gas ÷ tx gas)**
- Per-account cycle ≈ block_interval + ~0.5s confirm feed ⇒ sustainable TPS ≈ accountsN ÷ cycle.
- For **10k txs/block at ~2s blocks**: `targetTPS ≥ 5000`, `accountsN ≥ ~25-30k` (50k = comfortable
  headroom; utilization is never 100%), `workers: 256-512` (RPC concurrency only — LAN send latency
  ~10-20ms means ~100-150 in-flight HTTP calls at 5k TPS).
- **Chain-side prerequisites the loadtester cannot overcome** (check BEFORE blaming the tool):
  - Consensus `block.max_gas` bounds txs/block at `max_gas ÷ 21000` for value transfers — and
    lanes shrink it further: with `stable.params.max_blockspace_gas_weight = 50`, HALF the block
    gas is reserved for registered lanes, so default-lane value txs get only ~50%+overflow.
    (Observed on the 2026-07 devnet: hard ceiling ≈ 2,377 value txs ≈ 49.9M gas per block — the
    normal-lane share of a ~100M max_gas block. The local `init.sh` even writes max_gas=10M ≈ 476
    value txs.) **10k value txs/block needs the default lane to see ≥ 210M gas** — e.g.
    `max_gas ≈ 420M` at weight 50, or a lower reserved weight.
  - CometBFT `config.toml mempool.size` (default **5000**, devnet does not override) must exceed
    the per-block target with margin (≥ 2×) on the submit RPC nodes AND validators/fullnodes on
    the gossip path; a 5000-cap mempool physically cannot feed 10k-tx blocks. `max_txs_bytes`
    (default 1GB) and block `max_bytes` (default ~21MB) are fine for 10k small txs.
  - Validators run a NoOpMempool and PULL txs from fullnodes (TxProvider); the RPC/fullnode tier
    is where mempool.size and CheckTx throughput matter.
  - At 10k × 21k = 210M gas per block, EXECUTION time per block will stretch well beyond a 650ms
    `timeout_commit` — expect the real block interval to grow with block fullness; the engine's
    confirmation feed adapts automatically.
  - Watch `outcomes.mempool-full-backoff` in report.json — non-zero means the node's mempool cap
    (not the sender) throttled the run.
- Verify what a block actually held: `eth_getBlockByNumber(h, false)` transaction-hash count, or
  the lane table's per-block attribution when a cometRPC is configured.

## Common mistakes
- Continuous mode (`durationSec: 0`) when you wanted a PASS/FAIL — it returns LIVE, never a verdict.
- Setting `grpc` to a TLS endpoint → preconfigured run aborts (insecure-only dial). Leave it empty unless you have a plaintext gRPC.
- Treating `targetInflight` as per-account depth — it is now a relative workload weight. Set total rate with `targetTPS` and RPC concurrency with `workers`.
- Using `accountsN` as worker count — accounts rotate independently through the bounded worker pool. A large reusable pool reduces per-account nonce pressure without creating tens of thousands of goroutines/connections.
- Committing `accountSeed` — it deterministically controls every load-account private key. Commit only `accountsFile` (addresses/indexes/fingerprint).
