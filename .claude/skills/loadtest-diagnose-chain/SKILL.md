---
name: loadtest-diagnose-chain
description: Use when a stable-chain load test surfaces a chain anomaly - consensus halt, app-hash mismatch, mempool that won't drain, txs systematically stuck/rejected, or an unexpected verdict - and you need to locate the likely culprit in the chain's OWN code by dispatching read-only chain-code investigation agents and reasoning about which code is at fault.
---

# loadtest-diagnose-chain

When `loadtest-verify` says the CHAIN (not the loadtester config) misbehaved,
find the likely culprit in the chain source. You drive read-only investigation
agents over the chain repos, form a hypothesis, and decide chain-bug vs
loadtester-bug. Don't guess from memory — make agents cite `file:line`.

## Chain repos (on disk, siblings of this repo)
- `../../stable` — app: `app/blockspace/{handler,selector}.go` (lane quota / proposal), `app/checktx.go`, `app/mempool.go`, `app/ante/`, `precompiles/`.
- `../../stable-evm` — cosmos-evm fork: `ante/evm/` (nonce/sequence), `mempool/`, `rpc/` (eth RPC backend).
- `../../stable-sdk` — cosmos-sdk fork: `x/auth/ante/sigverify.go` (sequence), `baseapp/`.
- `../../stable-bft` — cometbft fork: `mempool/clist_mempool.go` (CList + recheck), `consensus/state.go` (block production).
- `../../stable-geth` — geth fork: tx types, EVM.

## Symptom → where to look first
| symptom | likely area |
|---|---|
| txs rejected / "nonce gap" / can't sustain depth | `stable-evm/ante/evm/09_increment_sequence.go`, `stable-sdk/x/auth/ante/sigverify.go` |
| CList won't drain while height advances | `stable/app/blockspace/handler.go` (TxProvider pull), `stable-bft/mempool/clist_mempool.go` (recheck/eviction) |
| lane quota NOT enforced / wrong skips | `stable/app/blockspace/selector.go` (quota math), `handler.go` (PrepareProposal) |
| app-hash mismatch / halt | `stable-bft/consensus/state.go`, `stable/app` Begin/EndBlock + any nondeterministic state (map iteration, time, floats) |
| no blocks produced under load | `stable-bft/consensus/state.go` (create_empty_blocks / TxsAvailable), TxProvider in `stable/app` |
| 2D-nonce / VIP / unordered behavior | `stable/app/checktx.go`, `stable-evm` 2D-nonce, `stable/precompiles/noncekey` |
| eth RPC returns wrong/zero (e.g. txpool) | `stable-evm/rpc/backend/` |

When symptoms co-occur, lead with the most upstream: a large flat CList **and**
frozen account nonces **while height advances** = a true wedge (not a tail) →
start at the proposer pull path `stable/app/blockspace/handler.go`, then CList
recheck, then the ante. A flat CList ≤ ~max(16, peak/1000) is just a residual
tail (REVIEW), not a wedge — don't escalate it.

## Workflow
1. **Capture evidence** from the run before it's gone: the failing `report.json` verdict + `reasons`, exact CometRPC/eth numbers (heights, `num_unconfirmed_txs`, per-account nonces), and any log lines. Mempool/CList state is in-memory — record counts now.
2. **Rule out the loadtester first.** Many "chain" symptoms are the tool's config/behavior (see `../loadtest-stable/reference.md` "Known limitations"): a residual tail is TxProvider-by-design, INCONCLUSIVE is environment-not-failure, a stuck nonce can be the tool abandoning a tx. Only escalate genuine chain misbehavior.
3. **Dispatch read-only chain-code investigators** (one per independent area; parallel if several). Give each the symptom + evidence + the repo paths above; require `file:line` citations and a verdict (expected-behavior / real-bug / inconclusive). Template below.
4. **Re-verify in the main session.** Read the agent's cited code yourself before accepting a conclusion — agents can be confidently wrong. For high-stakes claims use 2+ independent investigators (different framing) and accept only what survives.
5. **Decide & report**: chain-bug (cite the code + why) vs loadtester-bug (then fix here) vs expected-behavior. State confidence and the single most load-bearing code fact.

## Investigator subagent prompt template
> READ-ONLY (do not edit). Investigate a stable-chain symptom. Repos: `../../stable`, `../../stable-evm`, `../../stable-sdk`, `../../stable-bft`, `../../stable-geth`.
> OBSERVED: `<symptom + exact evidence: heights, n_txs, nonces, log lines, verdict>`.
> HYPOTHESIS to confirm or refute: `<your guess>`.
> Trace the relevant path and answer with `file:line` citations: (1) what the code does in this case, (2) is the symptom expected behavior or a bug, (3) which layer is responsible. VERDICT: expected / bug / inconclusive + the one code fact that decides it. Don't speculate beyond cited code.

## Common mistakes
- Blaming the chain for a loadtester artifact (residual tail, abandoned-nonce, assumed-params) — check reference.md limitations first.
- Accepting an agent's claim without reading the cited code.
- Investigating from memory instead of the on-disk fork (the forks differ from upstream — e.g. stable uses a plain PriorityNonceMempool, not the geth txpool).
- Losing volatile evidence (mempool/nonce state) by restarting the chain before capturing it.
