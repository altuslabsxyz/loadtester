# loadtester setup & target.yaml reference

How to configure a run, what each field does, how transactions are sent, and how
to cap spend. For behavior/why, see `reference.md`.

## target.yaml schema

```yaml
name: <label>                 # report label
chainId: <uint>               # EVM eip155 id; MUST match the node (run aborts on mismatch). required
cosmosChainId: ""             # informational
nodes:                        # >=1; jsonrpc required. cometRPC/grpc optional.
  - name: <n>
    role: fullnode            # fullnode (load sent here) | validator | vip (ONLY endpoint VIP txs go to)
    jsonrpc: https://...      # required
    cometRPC: https://...     # optional; CometBFT RPC = the ONLY mempool-depth signal (Goal 2) + app-hash (Goal 3) + block.max_gas (Goal 1 quota)
    grpc: ""                  # optional; cosmos gRPC for on-chain lane params. INSECURE-only dial -> leave "" for TLS testnets
funding:
  masterKey: "0x<hex>"        # funded key; signs only the funding txs (preconfigured mode)
  accountsN: <int>            # number of load accounts (= concurrency; each is 1-in-flight)
  fundPerAccount: "<int>"     # WHOLE gas tokens per account (integer string; tool multiplies by 1e18)
governance:
  mode: preconfigured         # testnet: lanes already registered, declared below. (fast-pass/real-vote = local only)
  proposerKey: ""             # fast-pass/real-vote only
  voterKeys: []               # fast-pass/real-vote only (must cover quorum)
  deposit: "...astable"       # fast-pass/real-vote only
  votingNode: 0
blockspace:                   # OPTIONAL. Omit -> built-in preset. Declare the ALREADY-REGISTERED lanes for a preconfigured testnet.
  maxBlockspaceGasWeight: 50  # % of block gas reserved for lanes
  lanes:
    - { id: 1,  name: vip,   weight: 20, vip: true }
    - { id: 10, name: erc20, weight: 30, toAddrs: ["@token0"], methods: ["0xa9059cbb"], txTypes: ["DYNAMIC_FEE"] }
    # quota(lane) = block.max_gas * maxBlockspaceGasWeight% * weight%
    # addrs: 0x.. or @name deploy ref; methods: 4-byte hex; txTypes: LEGACY|ACCESS_LIST|DYNAMIC_FEE|SET_CODE|TWO_D_NONCE
workload:
  durationSec: 120            # >0 = one-shot (yields a verdict). <=0 = continuous (LIVE, NO verdict)
  allowDestructive: false     # gate bump/selfdestruct (keep false on testnet)
  lanes:                      # which tx kinds to send + per-account target (see kinds below)
    value:     { targetInflight: 30 }
    unordered: { targetInflight: 20 }
    # erc20Transfer / swap / vip / bump / selfdestruct (toggle by presence; enabled: false to disable)
observe:
  pollIntervalMs: 1000        # collector poll cadence
  drainWindowSec: 60          # one-shot drain wait
  reportIntervalSec: 60       # continuous-mode snapshot cadence
logPaths:                     # local only: proposer/validator log file(s) for Goal 1/3 ground truth. empty on testnet.
  - /tmp/initsh.log
```

Defaults if omitted: `governance.mode=preconfigured`, `pollIntervalMs=200`,
`drainWindowSec=60`. Only `chainId` + one `jsonrpc` are strictly required.

## Workload kinds (each value/vip/unordered sends 1 wei)

| kind | tx | needs | lane |
|---|---|---|---|
| value | native 1-wei transfer | nothing | normal |
| vip | 2D-nonce VIP tx | a `role: vip` node | vip lane |
| unordered | 2D-nonce, NonceKey=MaxUint64, future timeout (STAB-185) | nothing | normal |
| erc20Transfer | `token.transfer` | a mintable test token in deployment.json | erc20 lane |
| swap | uniswap callee swap | pool+callee in deployment.json | swap lane |
| bump / selfdestruct | Destructible calls | Destructible + `allowDestructive: true` | normal |

Token-free set (`value`, `vip`, `unordered`) needs no deployed contracts → use
`-d <empty {}>`. `vip` auto-skips without a `role: vip` node.

## Capping spend (how to use only N of a funded account)

There is no spend-cap field. In `preconfigured` mode the master's ONLY outflow is
funding, so:

```
master outflow  =  accountsN x fundPerAccount   (in WHOLE gas tokens)
```

Set that product ≤ your budget. **Units:** whole tokens. `fundPerAccount: "1"` =
1 whole token = `1e18` atomic (e.g. 1 USDT0 = 1e18 ausdt0). So `accountsN: 50,
fundPerAccount: "1"` ⇒ master spends 50 whole tokens. The master-balance precheck
aborts (no spend) if it can't cover `product + gas`.

**Funds are NOT recovered**: each load account is a fresh random key; whatever is
funded to it is effectively spent (gas is tiny, the rest is stranded). So fund
modestly — accounts only need enough for their gas. More `accountsN` = more load
(and slower lockstep funding); larger `fundPerAccount` = more waste. Prefer many
accounts × small fund (e.g. `50 × 1`) over few × large.

## Procedure (preconfigured testnet)

```bash
make install                              # or: make build -> ./bin/loadtester
loadtester config -t target.yaml          # preflight: chainId, endpoints, vip endpoint, lanes, masked key+addr
loadtester start -t target.yaml -d deployment.json -o out --fail-on review
# read out/report.json: verdicts{goal1,goal2,goal3,overall} + reasons + paramsVerified/lanesReconciled/continuous
```
- `--fail-on review|fail|none`: process exits non-zero when overall meets the threshold (one-shot only).
- continuous (`durationSec<=0`) never yields a verdict — use a positive `durationSec` to get one.

## Reading the report (what a skill should surface)
From `out/report.json`:
- `verdicts.overall` + per-goal, and `reasons.{goal1,goal2,goal3}` (one-line why).
- `paramsVerified` false ⇒ lanes assumed (no gRPC). `lanesReconciled` false ⇒ declared lanes don't match on-chain.
- `continuous` true ⇒ this is a LIVE snapshot, not a verdict.
- Markdown's lane table flags **NOT EXERCISED** lanes (declared/registered but no traffic — NOT verified).
Relay PASS/FAIL plainly, and call INCONCLUSIVE/NOT_EVALUATED "not proven", not "fine".

## Worked example — public testnet, token-free, cap 50 USDT0 (validated)

```yaml
name: stable-testnet
chainId: 2201
cosmosChainId: ""
nodes:
  - { name: rpc, role: fullnode, jsonrpc: https://rpc.testnet.stable.xyz, cometRPC: https://<cosmos-rpc-host>, grpc: "" }
funding:
  masterKey: "0x<your funded key>"
  accountsN: 50
  fundPerAccount: "1"         # 50 x 1 = 50 USDT0 max outflow
governance:
  mode: preconfigured
workload:
  durationSec: 120
  allowDestructive: false
  lanes:
    value:     { targetInflight: 30 }
    unordered: { targetInflight: 20 }
observe:
  pollIntervalMs: 1000
  drainWindowSec: 60
```
This run sent ~6300 txs, drained the CList to 0 (Goal 2 PASS), reported Goal 1/3
INCONCLUSIVE (no logs / single endpoint), spent ~50 USDT0. To prove Goal 1 you
need `blockspace.lanes` declared + reachable gRPC + enough load to exceed a lane
quota; for Goal 3 you need ≥2 node endpoints.

## Local init.sh example (3 nodes, lane enforcement provable)
Use `role: vip` node (8555), set `logPaths: [/tmp/initsh.log]` (the foreground
validator's log = proposer skip-log source), `governance.mode: fast-pass` with the
init.sh keys, and declare `blockspace.lanes` with a small `weight` to make the
quota easy to exceed. See target.local.yaml in the repo for endpoints/keys.
