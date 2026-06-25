---
name: loadtest-stable
description: Run the stable-chain loadtester against a target and report a clear PASS/FAIL/REVIEW verdict. Use when asked to load-test a stable chain, verify Guaranteed-Blockspace lanes / mempool drain / determinism, or check that a (testnet) chain behaves correctly under load given a target YAML. On a real testnet the lanes are already registered (governance.mode: preconfigured) and declared in the YAML; this skill only verifies behavior and reports - it does not submit governance proposals.
---

# loadtest-stable

Drive EVM load at a stable chain and report whether it upholds three goals:
1. **Goal 1 - Guaranteed Blockspace**: per-lane gas quota is enforced.
2. **Goal 2 - Selective-Recheck**: the mempool drains; no stuck txs.
3. **Goal 3 - Non-determinism**: nodes never diverge on app_hash / no halt.

The tool is the `loadtester` binary in this repo. This skill runs it and turns
`out/report.json` into a one-shot verdict, and (optionally) spawns a read-only
chain-side monitor that reports chain problems independent of the load result.

## Read these as needed
- `reference.md` — how it works + the stable-chain facts that drive behavior
  (CList-only mempool, 1-in-flight nonces, decimals, TxProvider inclusion, VIP,
  proposer logs, gov modes, verdict vocabulary, known limitations). Read this to
  understand WHY a run produces its verdicts.
- `config.md` — full `target.yaml` schema, the workload kinds, how to send txs,
  how to CAP spend, the preconfigured-testnet procedure, and worked examples.
- `chain-monitor.md` — subagent prompt template for the independent chain-health monitor.

## When to use
- "Load-test the stable chain at <target>." / "Verify lanes on the testnet."
- "Does the chain drain / stay deterministic under load?"
- After someone registered lanes on a testnet and wants them exercised + verified.

## The real-world (testnet) contract
On a real testnet the operator does NOT submit a gov proposal. Lanes are already
registered; the operator declares them in the target YAML under `blockspace.lanes`
and sets `governance.mode: preconfigured`. This skill verifies against those
already-registered lanes - it never proposes. If `node.grpc` is set, the tool
RECONCILES the declared lanes against on-chain params and warns on a mismatch.

## Procedure

### 1. Build
```bash
make install     # -> `loadtester` on PATH   (or: make build -> ./bin/loadtester)
```

### 2. Preflight (static) - confirm the settings before a long run
```bash
loadtester config -t <target.yaml>
```
Check the printed summary: chainId, the load endpoint, the **vip endpoint**
(VIP txs need a `role: vip` node - else they're skipped), the lane source
(config blockspace vs preset), `governance.mode: preconfigured` for testnet, and
that the master key's derived address is the funded one. Abort if anything is wrong.

### 3. Run a BOUNDED one-shot (so it returns a verdict)
Continuous mode (`durationSec: 0`) never yields a verdict (Goal 2 is LIVE). For a
report, the target must have `workload.durationSec > 0`. Then:
```bash
loadtester start -t <target.yaml> -d <deployment.json> -o out --fail-on review
```
- `-d` deployment.json supplies contract addresses for erc20/swap/bump/selfdestruct
  workloads. For token-free workloads (`value`, `vip`, `unordered`) an empty `{}`
  is fine.
- `--fail-on review` exits non-zero if overall is FAIL or REVIEW; use `fail` for
  FAIL-only, `none` to never fail the process.

### 4. Report from `out/report.json` (do NOT parse the Markdown)
Read these fields and report them plainly:
- `verdicts`: `goal1`/`goal2`/`goal3`/`overall` in {PASS, FAIL, REVIEW,
  INCONCLUSIVE, NOT_EVALUATED, LIVE}.
- `reasons`: a one-line "why" per goal - surface these.
- `paramsVerified`, `lanesReconciled`: if false, say lane verification is weak /
  the declared lanes don't match chain.
- `continuous`: if true, the run never produced a real verdict (tell the user to
  set a positive `durationSec`).
- `lane.peakLaneGas` vs `lane.quota`, and the Markdown's "NOT EXERCISED" rows: a
  declared/registered lane with no traffic was NOT load-tested (the generator only
  makes value/erc20/swap/vip/unordered shapes) - call this out; do not imply it passed.

Verdict vocabulary to relay honestly:
- **PASS** only when actually proven. **INCONCLUSIVE/NOT_EVALUATED** mean "not
  proven", not "fine" - on a JSON-RPC-only or no-logs testnet, Goal 1/3 are often
  INCONCLUSIVE by nature. Say so.
- **REVIEW** = needs a human look (e.g. residual mempool tail, assumed params,
  height stall).

### 5. (Recommended) Spawn a chain-side monitor - reports chain problems separately
The load verdict and chain HEALTH are different concerns. Before/at step 3, also
dispatch a read-only subagent that watches the chain itself and reports problems
independent of the load result. Use the prompt in `chain-monitor.md` (same dir),
filling in the node endpoints / log paths from the target YAML. Run it in the
background during the load and fold its findings into your report under a separate
"Chain health" heading.

## Notes / gotchas (verified against the chain)
- **Mempool depth = CometBFT `num_unconfirmed_txs`** (a `cometRPC` must be set).
  The EVM `txpool_*` RPCs are vestigial on stable (always 0) - never used. Without
  a CometRPC, Goal 2 is NOT EVALUATED.
- **1-in-flight per account**: stable rejects future nonces (no queue). Load scales
  with `funding.accountsN`, not per-account depth. To push a lane harder, raise
  accountsN and the lane's `targetInflight`, not depth.
- **VIP** needs a `role: vip` node; its nonce-key lane id is taken from on-chain.
- **logPaths** are local-only ground truth for Goal 1/3. init.sh runs the
  validator in the foreground, so it does NOT write a validator log file - point
  logPaths at the files that ARE written (e.g. /tmp/stabled-normal.log) or omit
  them on testnet (Goal 1/3 then degrade honestly). Stale files (mtime before the
  run) are skipped and noted.
