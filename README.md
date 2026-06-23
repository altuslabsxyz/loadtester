# loadtester (`loader` CLI)

Generic load-testing harness for an **EVM + CometBFT** chain, as a cobra CLI.
It drives high-throughput EVM transactions across many accounts and observes:

1. **Mempool drain** - the mempool empties after load (no stuck txs).
2. **Node consistency** - nodes stay in agreement on `app_hash` and the chain
   does not halt under load (a liveness/consistency check; see the note in the
   report about why this is not a full determinism proof).

> Chain-specific QA (Guaranteed Blockspace lane quotas, governance lane
> registration, VIP / unordered 2D-nonce txs) lives on the **`stable`** branch,
> which depends on the `stable` chain repo. `main` is chain-agnostic.

## Build & run

```bash
go build -o loader .
./loader start --target target.local.yaml --deployment deployment.json --out out
./loader start --help
```

Flags: `-t/--target`, `-d/--deployment`, `-o/--out`.

## Architecture

```
target.yaml ──┐                      ┌─ MempoolCollector  (CList + txpool depth)
              ├─► harness (phases) ──┤
deployment.json┘  setup→load→        └─ AppHashCollector  (per-node app_hash, halt)
                  observe→report
```

- `target.yaml` describes one environment (nodes, funding key, workload mix,
  observe cadence). Local and testnet are just different files.
- `deployment.json` provides contract addresses (factory, pool, tokens, callee,
  destructible) for the contract workloads.

## Workloads

Toggle each via `workload.lanes` (omit a key or set `targetInflight: 0`):

- `value` - native value transfer
- `erc20Transfer` - ERC20 `transfer`
- `swap` - uniswap-v3 swap via the test callee
- `bump` / `selfdestruct` - `Destructible` contract calls (need
  `allowDestructive: true`)

The driver is open-loop: each account submits without waiting for receipts,
bounded by a per-account in-flight window, so throughput scales with accounts.

## One-shot vs continuous

- `workload.durationSec > 0`: run that long, then adaptive drain, then a final
  report.
- `workload.durationSec <= 0`: run until Ctrl+C, writing a report snapshot every
  `observe.reportIntervalSec`, plus a final report on stop.

## Step 1 - deploy contracts (optional, for contract workloads)

The TS deployer (`ts/`) deploys uniswap-v3 + test ERC20 + `Destructible` and
writes `deployment.json`. Run it inside a uniswap-v3-core hardhat project:

```bash
cp /abs/path/loadtester/ts/Destructible.sol contracts/test/
npx hardhat compile
STABLE_RPC_URL=http://127.0.0.1:8545 STABLE_CHAIN_ID=999 \
LT_DEPLOYMENT_OUT=/abs/path/loadtester/deployment.json \
npx hardhat run /abs/path/loadtester/ts/deploy.ts --network stable
```

For `value`-only load, a minimal `deployment.json` (`{}`) is enough.
