---
name: loadtest-verify
description: Use when interpreting or reporting stable-chain load-test results - reading report.json verdicts/reasons, deciding overall PASS/FAIL/REVIEW/INCONCLUSIVE, judging whether the chain actually upheld the three goals, or running the independent chain-health monitor alongside a run.
---

# loadtest-verify

Turn a finished run into an honest verdict and report. Assumes the run is done
(`loadtest-send`). If a result points to a chain problem, escalate with
`loadtest-diagnose-chain`.

**Reference:** `../loadtest-stable/reference.md` (verdict vocabulary + why each goal resolves as it does).

## Read `out/report.json` (not the Markdown)
- `verdicts.{goal1,goal2,goal3,overall}` and `reasons.{goal1,goal2,goal3}` (one-line why).
- `paramsVerified` (false ⇒ lanes ASSUMED, no gRPC), `lanesReconciled` (false ⇒ declared lanes ≠ on-chain).
- `continuous` (true ⇒ a LIVE snapshot, NOT a verdict — rerun one-shot to judge).
- Markdown lane table: lanes flagged **NOT EXERCISED** were declared/registered but got no traffic → NOT verified; never report them as passed.

## Verdict meaning (report honestly)
| verdict | means |
|---|---|
| PASS | actually proven |
| FAIL | a goal was violated — real problem |
| REVIEW | needs a human look (residual mempool tail, assumed params, height stall) |
| INCONCLUSIVE / NOT_EVALUATED | **not proven — NOT "fine"** |
| LIVE | continuous mode; no pass/fail produced |

- `overall` = FAIL if any FAIL, else REVIEW if any REVIEW, else INCONCLUSIVE if any goal unproven, else PASS. A single PASS never masks an unproven goal.
- On a JSON-RPC-only / no-logs / single-endpoint testnet, **Goal 1 (needs proposer logs) and Goal 3 (needs ≥2 nodes) are INCONCLUSIVE by nature** — that is honest, not a chain failure. Say "not proven here", not "passed".
- Goal 2 PASS = CList drained to 0. A small residual tail is REVIEW (TxProvider didn't pull it), not necessarily a wedge; a large flat backlog with advancing height is the real FAIL.

## Chain health is a SEPARATE question from the load verdict
Run the read-only chain-side monitor (`../loadtest-stable/chain-monitor.md`)
during/after the load and report its findings under a separate "Chain health"
heading. A clean load verdict can still coexist with a sick chain and vice versa.

## When to escalate to loadtest-diagnose-chain
- Goal 2 FAIL (mempool genuinely not draining while height advances), Goal 3 FAIL (app-hash mismatch / halt), txs systematically rejected, or any verdict that implies the CHAIN (not the loadtester config) misbehaved.

## Common mistakes
- Reporting INCONCLUSIVE/NOT_EVALUATED as success.
- Parsing the Markdown instead of `report.json`.
- Calling a small residual-tail REVIEW a chain failure.
- Ignoring `paramsVerified=false` / `lanesReconciled=false` when stating Goal 1.
