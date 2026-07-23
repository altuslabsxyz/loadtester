# stable-loadtester (`loadtester` CLI)

Standalone QA harness for the Stable chain, as a cobra CLI. **Not** part of the
chain release. Lives beside the `stable` repo (depends on it via a local
`replace ../stable` in go.mod).

```bash
make install              # build + install `loadtester` onto PATH (go install)
make config               # scaffold an editable target.yaml from target.example.yaml
$EDITOR target.yaml       # set your endpoints, master key, workload mix
loadtester config         # print the resolved settings to verify them
loadtester start          # reads target.yaml by default
loadtester start --help

# alternatives
make build                # build into ./bin/loadtester (no install)
loadtester start -t target.testnet.yaml -d deployment.json -o out --fail-on fail
```

`make config` copies `target.example.yaml` (a fully commented template of every
settable value) to `~/.loadtester/target.yaml` (created if missing, never
overwritten). `loadtester config` then prints the resolved settings (defaults
applied, validated; private keys masked, their addresses shown) so you can
confirm what `start` will use. Both `start` and `config` default to
`--target ~/.loadtester/target.yaml`; pass `-t` to use a file elsewhere.

> `make config` scaffolds the file; `loadtester config` inspects it.

**VIP txs** are only accepted by a dedicated endpoint, so they are sent to the
node with `role: vip`. List one to enable the `vip` workload; if no `role: vip`
node is configured, VIP txs are skipped (logged at start, shown by `config`).

Verifies three things under load:

1. **Guaranteed Blockspace** - per-lane gas quota is enforced.
2. **Selective-Recheck** - the mempool drains; no stuck pending txs.
3. **Non-determinism** - nodes never diverge on `app_hash` (BlockSTM / MemIAVL),
   including SELFDESTRUCT / same-slot-contention edge cases.

Design spec: `docs/superpowers/specs/2026-06-22-load-tester-design.md`.

## Architecture

```
target.yaml ──┐                         ┌─ LaneAttributionCollector (quota)
              ├─► Go harness (phases) ──┼─ MempoolCollector (drain/stuck)
deployment.json┘   setup→lanes→load→     └─ AppHashCollector (node divergence)
   ▲               observe→report
   │
TS deployer (uniswap-v3-core hardhat) ── deploys Factory/pool/ERC20/Destructible
```

- Environment is one `target.yaml`. Local `init.sh` chain and a remote testnet
  are just different target files - no code change.
- Lanes are registered via the **gov precompile** (`MsgUpdateParams`), mirroring
  `scripts/stable-client-txtrigger`. On testnet use `real-vote` or
  `preconfigured`.

## Step 1 - deploy contracts (TS)

The deployer runs inside the uniswap-v3-core hardhat project (it has the
contracts, typechain, and the `stable` network).

```bash
# from the uniswap-v3-core repo
cp /abs/path/stable-loadtester/ts/Destructible.sol contracts/test/
npx hardhat compile

STABLE_RPC_URL=http://127.0.0.1:8545 STABLE_CHAIN_ID=999 \
LT_DEPLOYMENT_OUT=/abs/path/stable-loadtester/deployment.json \
npx hardhat run /abs/path/stable-loadtester/ts/deploy.ts --network stable
```

This writes `deployment.json` (factory, pool, tokens, callee, destructible).

## Step 2 - run the load test (`loadtester`)

```bash
make install
loadtester start -t target.yaml -d deployment.json -o out
```

Flags: `-t/--target` (default `target.yaml`), `-d/--deployment` (default
`deployment.json`), `-o/--out` (default `out`), `--fail-on` (`none|fail|review`).

Phases: connect + chainId sanity check → fund N accounts → mint/approve test
tokens → register lanes (governance) → start collectors → drive workloads for
`durationSec` → wait `drainWindowSec` → write `out/report.md` + `out/report.json`.

### Reusable high-rate account pools

Account count, concurrency, and rate are independent controls. For example,
50,000 accounts at 5,000 tx/s revisit an account about once every ten seconds,
while `workers: 256` bounds client/RPC concurrency:

```yaml
funding:
  accountsN: 50000
  accountSeed: "0x<at-least-32-random-secret-bytes>" # signing authority; never commit
  accountsFile: "accounts/load-pool.json"            # public addresses only
  fundPerAccount: "0.01"
workload:
  workers: 256
  targetTPS: 5000
```

The same seed and index regenerate the same signing key. The manifest contains
only versioned public addresses and a seed fingerprint, never private material.
Later runs query balances with bounded concurrency and top up only deficits.
Seeded pools default to `sweepBack: false`, retaining funds for reuse.

Ordered sends run through a conflict-free engine (`workload/engine.go`). On
this chain `eth_getTransactionCount("pending")` never reflects the mempool (the
EVM txpool is vestigial) and each sender may hold at most ONE ordered tx per
nonce-key, so the engine keeps its own ownership state: an account is eligible
only while it has no tx in flight; an accepted send parks it until the
block-hash feed (one hashes-only block fetch per new height) or a
committed-nonce probe proves the slot resolved. The only same-nonce resend the
engine can produce is a deliberate replacement — fee-bumped ~1.3x after a 90s
TTL — so the mempool's "tx already in mempool" and "doesn't fit the replacement
rule" rejections cannot be triggered by ordinary scheduling. A full worker
queue drops that scheduling opportunity instead of accumulating stale work, so
`targetTPS` is a cap and target, not a throughput guarantee. Per-run send
outcomes (duplicates, conflicts, backpressure, replacements) are counted in
`report.json` under `outcomes` and logged every 15s; a healthy run shows ~zero
conflict counters, and `mempool-full-backoff` marks explicit chain backpressure.

### Workload kinds

`value`, `erc20Transfer`, `swap`, `vip` (2D-nonce VIP), `bump` / `selfdestruct`
(determinism, need `allowDestructive`), and `unordered` (2D-nonce unordered tx:
NonceKey=MaxUint64, Nonce=0, unique future timeout - exercises the
selective-recheck/STAB-185 eviction path). Toggle each via `workload.lanes`
(omit a key or set `targetInflight: 0`). `unordered` is fire-and-forget and
shares the same aggregate worker/TPS bounds. `targetInflight` is a deterministic
relative mix weight for every kind.

### Config-driven lanes (general use)

By default the harness registers a built-in preset (erc20 + swap + vip lanes).
To define arbitrary lanes without code, add a `blockspace:` block to the target:

```yaml
blockspace:
  maxBlockspaceGasWeight: 50
  lanes:
    - { id: 1,  name: vip,   weight: 20, vip: true }
    - { id: 10, name: erc20, weight: 30, toAddrs: ["@token0"], methods: ["0xa9059cbb"], txTypes: ["DYNAMIC_FEE"] }
    - { id: 20, name: swap,  weight: 20, toAddrs: ["@callee"], methods: ["0x128acb08"], txTypes: ["DYNAMIC_FEE"] }
```

- Addresses accept `0x..` or `@name` deployment refs (factory, callee,
  destructible, gasToken, token symbols, `token0`, `pool0`, or any key in
  deployment.json `contracts`). Methods are 4-byte hex selectors.
- **Toggle a lane** (e.g. swap) by adding/removing its entry. Its registration
  is no longer hardcoded.
- Each workload's expected lane is **derived automatically** by classifying a
  sample tx against the registered params - no manual workload->lane mapping.
- Lane registration is idempotent: a config whose lane ids are already on-chain
  is reused; a changed id set is re-registered (governance modes).

## Running against a public testnet (JSON-RPC only)

A real testnet typically exposes only EVM JSON-RPC - no node log files, often no
CometBFT RPC, no gRPC, and no validator keys. The harness degrades to that:

1. Copy `target.testnet.yaml`, fill in `chainId`, node `jsonrpc` endpoints, and a
   funded `masterKey`. Leave `cometRPC`/`grpc` empty if unreachable.
2. Keep `governance.mode: preconfigured`. Without gRPC the lane params can't be
   read on-chain, so the harness assumes the config-declared lanes and the report
   flags Goal 1 as **assumed, not verified**.
3. Default workloads are token-free (`value`, `vip`, `unordered`) so it runs
   against a bare endpoint. To enable `erc20Transfer`/`swap`, first deploy the
   harness `TestERC20`/pool via the TS deployer and point `deployment.json` at it
   (otherwise the mint reverts and setup aborts with a clear error).
4. `loadtester start -t target.yaml` (continuous by default). For CI, run a
   one-shot (`durationSec > 0`) with `--fail-on=fail` (or `review`).
5. Funds recovery: ephemeral random pools default to `sweepBack: true`; each
   account returns its leftover balance after a **one-shot** run. Seeded pools
   default to `false` so balances remain available on the next run, which tops
   up only underfunded accounts. The seed is required to recover or reuse those
   balances and must be backed up securely.

What is observable on a JSON-RPC-only target:

- **Goal 1** (lane quota): block gas limit is read via `eth_getBlockByNumber`
  (JSON-RPC fallback for CometBFT `block.max_gas`); enforcement itself is
  UNPROVABLE without node logs - attribution is an upper bound only.
- **Goal 2** (mempool drain): observed via CometBFT `num_unconfirmed_txs` (CList
  depth). The EVM txpool RPCs (`txpool_status`/`content`) are vestigial on the
  stable chain (always 0) and are not used. Without a reachable CometRPC there is
  no trustworthy mempool signal, so Goal 2 is **NOT EVALUATED** (never a false PASS).
- **Goal 3** (determinism): liveness/halt is tracked via `eth_blockNumber`;
  cross-node app-hash comparison needs CometRPC and is otherwise unavailable
  (verdict INCONCLUSIVE on a healthy chain, REVIEW on a halt).

### CI gating

`report.json` includes a `verdicts` block (`goal1`/`goal2`/`goal3`/`overall`,
each PASS/FAIL/REVIEW/INCONCLUSIVE/NOT_EVALUATED/LIVE). `--fail-on=fail|review`
makes `loadtester start` exit non-zero when the overall verdict meets the
threshold (one-shot only; continuous runs are LIVE and never trip it).

### One-shot vs continuous

- **One-shot** (`workload.durationSec > 0`): load for that long, then adaptive
  drain (polls until the mempool empties, capped), then a single final report.
- **Continuous** (`workload.durationSec <= 0`, e.g. `0`): load runs until you
  Ctrl+C (SIGINT). A fresh report snapshot is written every
  `observe.reportIntervalSec` (default 30s), and a final report on stop. No drain
  phase - Goal 2 shows live mempool depth ("LIVE (continuous)"), not a drain
  pass/fail (a full mempool under sustained load is expected). This is the mode
  for leaving load running on a testnet.
- Lane registration is **idempotent**: if the plan's lanes are already on-chain
  (e.g. registered by a prior run), submission is skipped and they are reused.
  For continuous testnet runs, register once then use `governance.mode:
  preconfigured` (or any mode - it will detect and skip).

## Reading the report

- **Goal 1**: per-lane peak `gasUsed` vs quota; quota "violations" are an upper
  bound (overflow can move txs to lower lanes) - confirm against node logs.
- **Goal 2**: PASS if both CList and EVM mempool drain to 0 after the window.
- **Goal 3**: PASS if all nodes agree on `app_hash` at every height. With one
  node it is INCONCLUSIVE (stall-only).

## Notes / limits

- EVM chainId is build-time (`app/config.EVMChainID`); local build = 999, set in
  `target.yaml`. The harness aborts on `eth_chainId` mismatch.
- The lane classifier mirrors `app/blockspace/selector.go` as of the spec date;
  keep in sync if matching logic changes.
- VIP txs use the stable-geth 2D-nonce (`CustomTx`) with the VIP bit set.
- Destructive workloads (`bump`, `selfdestruct`) need `allowDestructive: true`
  (off by default for testnet safety).
- Functional behavior must be validated against a live chain; `go build`/`go vet`
  only check compilation.
```
