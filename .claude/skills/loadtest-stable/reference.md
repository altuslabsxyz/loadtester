# loadtester reference — how it works (stable chain)

How the `loadtester` behaves and the stable-chain facts that drive its design.
Read this to understand WHY a run produces the verdicts it does. For setup/yaml
and how to send txs, see `config.md`. For the chain-side monitor, see
`chain-monitor.md`.

## Architecture & phases

One `target.yaml` fully describes an environment; one `deployment.json` supplies
contract addresses (empty `{}` is fine for token-free workloads). `loadtester
start` runs these phases in order:

1. **connect + chainId check** — dials the load endpoint; aborts on `eth_chainId` mismatch.
2. **fund** — `fundPerAccount` reaches each of `accountsN` fresh random accounts via a FAN-OUT TREE (~log2(accountsN) blocks - see 1-in-flight below).
3. **token prep** — `PrepareAccounts` mints/approves test tokens (only if the deployment has them; skipped for token-free workloads).
4. **lanes** — register (fast-pass/real-vote) OR read/assume (preconfigured) the lane params; build the classifier + per-lane quotas.
5. **observe** — start 3 collectors: Lane (per-lane gas attribution), Mempool (CList depth), AppHash (per-node app_hash + stall).
6. **load** — drive the workload mix for `durationSec` (one-shot) or until Ctrl+C (continuous).
7. **drain** — poll the CList until it empties (one-shot only), capped.
8. **report** — write `out/report.md` + `out/report.json` with per-goal + overall verdicts.

## The three goals

- **Goal 1 — Guaranteed Blockspace**: each lane's per-block gas quota is enforced.
- **Goal 2 — Selective-Recheck**: the mempool drains; no stuck txs.
- **Goal 3 — Non-determinism**: nodes never diverge on `app_hash`; no halt.

## Chain facts that drive the design (all verified)

### Mempool signal = CometBFT `num_unconfirmed_txs` (CList) ONLY
The stable app does NOT wire cosmos-evm's geth txpool, so the EVM `txpool_status`/
`txpool_content` RPCs are **vestigial — always 0**. The tool ignores them. The
only trustworthy mempool-depth signal is the CometBFT CList via a node's
`cometRPC`. **No `cometRPC` reachable → Goal 2 is NOT EVALUATED** (never a false PASS).

### 1-in-flight per account (future nonces are rejected, not queued)
The ante rejects `txNonce > accountNonce` with `ErrNonceGap` and there is NO
future/queued lane — a gapped tx is dropped. So an account can have at most ONE
in-flight tx. Consequences:
- The driver is **closed-loop**: send one tx → wait for its receipt → send the next.
- **Concurrency comes from the NUMBER of accounts (`accountsN`), not depth.** To push harder, raise `accountsN`, not per-account inflight.
- **Funding fans out as a tree**: one sender can still fund only one account per block (blasting from the master would get all-but-the-first dropped), so each round the master AND every already-funded account fund one new account — the funded set doubles per block. Intermediate accounts receive their own share plus everything they must forward, so the master float is unchanged. (Funding takes ~`log2(accountsN)` blocks, not ~`accountsN`; the sweep-back is likewise concurrent — distinct senders, one tx each.)
- `unordered` txs are exempt (NonceKey=MaxUint64) and are rate-limited fire-and-forget.

### Concurrent broadcasts can panic the node's CheckTx (account-creation race)
Two `eth_sendRawTransaction` calls processed concurrently by a node can race in
CheckTx's fee-deduction path when an account has to be created (x/auth's unique
account-number index trips: `collections: conflict: index uniqueness constrain
violation`, a recovered panic; the tx is rejected with the node's stack in the
RPC error). Observed on v1.8.0-rc0 during fan-out funding. The tool therefore
SERIALIZES its own broadcasts during funding and sweep (receipt waits stay
parallel - a broadcast is ~ms, so rounds still mine in ~one block) and RETRIES
transient rejections (same signed tx, idempotent; "already known" = success).
This is a CHAIN-side fragility - if it appears during the LOAD phase (workloads
broadcast concurrently by design), report it to the chain team rather than
tuning the tool around it.

### Native gas-token decimals
`fundPerAccount` is DECIMAL whole gas tokens (e.g. `"0.01"`, `"1"`, `"2.5"`); the
tool multiplies by `10^18` to wei (fractions beyond 18 decimals truncate). Fund
each account only what it needs for gas (~0.00002/tx) rather than a whole token.
This is correct only if the chain's EVM balance is 18-decimal. **The public testnet's
USDT0 is 18-dec** (`eth_getBalance` returns `whole × 1e18`). The local `init.sh`
chain configures a 6-dec gas token — verify per environment. If the master-balance
precheck aborts with "balance X < required", the decimals don't match the 18-dec
assumption (the run fails safe; no funds lost).

### Block production is pull-based (TxProvider) → CList depth ≠ included set
The validator builds blocks by PULLING txs from fullnodes' SDK mempool over a
TxProvider gRPC (height-lag / rate-limit / recheck), not from its own CList. So a
fullnode's CList can hold a small residual TAIL the proposer never pulls. The
tool treats a small flat tail (≤ ~max(16, peak/1000)) as **REVIEW (residual tail),
not FAIL**; a genuine large flat backlog is FAIL. Detect a true wedge by
account-nonce stall while height keeps advancing, not by raw CList count.

### VIP (2D-nonce) txs
VIP txs carry a 2D nonce-key (VIP bit + lane id) and are accepted ONLY by the
node with `role: vip` — they go to that node's RPC. The VIP lane id is taken from
the ON-CHAIN params (not the YAML), and the per-key sequence is seeded/resynced
from the noncekey precompile at `0x0000000000000000000000000000000000002D2D`
(`getNonce(account, nonceKey)`; `eth_getTransactionCount` only knows key 0).
**No `role: vip` node → the vip workload is skipped.**

### Goal 1 ground truth = proposer skip logs
Quota enforcement is PROVEN by the proposer's log line `skip tx: lane gas quota
exceeded` (it caps a lane and skips overflow). These are emitted by the
PROPOSER (validator). `init.sh` runs the validator in the FOREGROUND, so they
land wherever you redirected init.sh output (e.g. `/tmp/initsh.log`), NOT in a
`/tmp/stabled-validator.log` file. On a public testnet node logs are not
reachable → Goal 1 is INCONCLUSIVE (RPC attribution is an upper bound only). The
log scanner ignores files older than the run start (stale prior-run logs).

### Governance modes
- **preconfigured** (TESTNET): lanes are already registered; declare them in
  `blockspace.lanes` and the tool reads/assumes them. No proposal is submitted.
  With `node.grpc` set it RECONCILES declared vs on-chain (id + weight +
  matchers); a mismatch → `lanesReconciled=false` → Goal 1 capped at REVIEW.
- **fast-pass / real-vote** (LOCAL): submit a MsgUpdateParams proposal and vote.
  Needs `proposerKey`/`voterKeys` with quorum. Not used on a real testnet.
- **gRPC caveat**: the tool dials cosmos gRPC INSECURE-only, so a TLS public-testnet
  gRPC will fail. If you don't need on-chain lane verification, leave `grpc: ""`
  (preconfigured then assumes the declared/preset lanes).

## Verdict vocabulary (report.json `verdicts` + `reasons`)
`PASS` (proven) · `FAIL` · `REVIEW` (needs a human look) · `INCONCLUSIVE` /
`NOT_EVALUATED` (NOT proven — not "fine") · `LIVE` (continuous mode, no verdict).
`overall` is FAIL if any goal FAILs, else REVIEW if any REVIEWs, else
INCONCLUSIVE if any goal was left unproven, else PASS. **A single PASS never
masks an unproven goal.** On a JSON-RPC-only / no-logs / single-endpoint testnet,
Goal 1 and Goal 3 are INCONCLUSIVE by nature — that is honest, not a failure.

## Known limitations (don't mistake for bugs)
- The generator only produces value / erc20Transfer / swap / vip / unordered tx
  SHAPES; a lane whose matcher none of these hit is reported **NOT EXERCISED**
  (never silently "passed").
- Funds sent to the random load accounts are swept back to the master at the end
  of a one-shot run by default (`funding.sweepBack: true`); with it off, or in
  continuous mode (Ctrl+C interrupts first), they're stranded on in-memory keys
  and lost. Either way the full float is needed upfront (precheck).
- gRPC is insecure-only (no TLS); single VIP lane only; reconciliation checks
  declared⊆on-chain (extra on-chain lanes are not surfaced).
