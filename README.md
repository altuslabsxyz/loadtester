# stable-loadtester (`loader` CLI)

Standalone QA harness for the Stable chain, as a cobra CLI. **Not** part of the
chain release. Lives beside the `stable` repo (depends on it via a local
`replace ../stable` in go.mod).

```bash
go build -o loader .          # or: go install ./...
./loader start --target target.local.yaml --deployment deployment.json --out out
./loader start --help
```

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

## Step 2 - run the load test (`loader`)

```bash
go build -o loader .
./loader start -t target.local.yaml -d deployment.json -o out
```

Flags: `-t/--target` (default `target.local.yaml`), `-d/--deployment` (default
`deployment.json`), `-o/--out` (default `out`).

Phases: connect + chainId sanity check → fund N accounts → mint/approve test
tokens → register lanes (governance) → start collectors → drive workloads for
`durationSec` → wait `drainWindowSec` → write `out/report.md` + `out/report.json`.

### Workload kinds

`value`, `erc20Transfer`, `swap`, `vip` (2D-nonce VIP), `bump` / `selfdestruct`
(determinism, need `allowDestructive`), and `unordered` (2D-nonce unordered tx:
NonceKey=MaxUint64, Nonce=0, unique future timeout - exercises the
selective-recheck/STAB-185 eviction path). Toggle each via `workload.lanes`
(omit a key or set `targetInflight: 0`). `unordered` is fire-and-forget
(high-rate; its `targetInflight` caps the worker count, max 32), so keep it
modest relative to the other lanes.

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
