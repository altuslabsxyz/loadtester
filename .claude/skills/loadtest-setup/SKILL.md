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
3. **Cap spend** (critical on a real funded account): master outflow `= accountsN × fundPerAccount` in WHOLE gas tokens (`fundPerAccount:"1"` = 1 token = 1e18 atomic). Set the product ≤ your budget. Funds go to throwaway accounts and are NOT recovered — prefer many small accounts (e.g. `50 × 1`) over few large. The master-balance precheck aborts (no spend) if it can't cover `product + gas`.
4. **Preflight**: `loadtester config -t target.yaml` — verify chainId, the load + vip endpoints, lane source, `mode`, and that the masked masterKey's derived address is the funded one. Abort on anything wrong.

## Common mistakes
- Continuous mode (`durationSec: 0`) when you wanted a PASS/FAIL — it returns LIVE, never a verdict.
- Setting `grpc` to a TLS endpoint → preconfigured run aborts (insecure-only dial). Leave it empty unless you have a plaintext gRPC.
- Expecting per-account depth to add load — it can't (1-in-flight); raise `accountsN`.
- Funding large amounts "to be safe" — it's stranded/spent in random accounts.
