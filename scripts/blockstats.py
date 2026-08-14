#!/usr/bin/env python3
"""blockstats — block time and fill, straight from the chain's own headers.

Why not the node logs: the devnet's default log format stamps lines with
time.Kitchen (minute resolution), so a captured log cannot time a ~1s block, and
the JSON format only survives until someone restarts without the flag. The
CometBFT /blockchain endpoint returns each block's header time at nanosecond
resolution plus its tx count, which is the canonical answer and needs no
cooperation from whoever started the nodes.

Usage:
  scripts/blockstats.py --rpc http://10.10.30.11:26657 --last 200
  scripts/blockstats.py --rpc http://10.10.30.11:26657 --from 35463000 --to 35463400
"""
import argparse
import json
import urllib.request
from datetime import datetime


def rpc(base, path):
    with urllib.request.urlopen(base.rstrip('/') + path, timeout=20) as r:
        return json.load(r)


def parse_time(s):
    # RFC3339 with variable-width fractional seconds, e.g. 2026-08-14T03:35:44.123456789Z
    s = s.rstrip('Z')
    if '.' in s:
        head, frac = s.split('.', 1)
        frac = (frac + '000000')[:6]
        s = head + '.' + frac
    else:
        s = s + '.000000'
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%f")


def fetch(base, lo, hi):
    """Return [(height, time, num_txs)] for [lo, hi]. /blockchain serves 20 at a time."""
    out = {}
    h = hi
    while h >= lo:
        start = max(lo, h - 19)
        d = rpc(base, f"/blockchain?minHeight={start}&maxHeight={h}")
        metas = d['result']['block_metas']
        if not metas:
            break
        for m in metas:
            hdr = m['header']
            out[int(hdr['height'])] = (parse_time(hdr['time']), int(m['num_txs']))
        h = start - 1
    return out


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0
    i = min(len(sorted_vals) - 1, int(len(sorted_vals) * p))
    return sorted_vals[i]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rpc', default='http://10.10.30.11:26657')
    ap.add_argument('--last', type=int, default=0, help='analyze the last N blocks')
    ap.add_argument('--from', dest='lo', type=int, default=0)
    ap.add_argument('--to', dest='hi', type=int, default=0)
    ap.add_argument('--min-txs', type=int, default=0,
                    help='restrict the summary to blocks with at least this many txs (isolates the load window)')
    ap.add_argument('--csv', default='', help='write per-block rows here')
    args = ap.parse_args()

    head = int(rpc(args.rpc, '/status')['result']['sync_info']['latest_block_height'])
    hi = args.hi or head
    lo = args.lo or (hi - args.last + 1 if args.last else hi - 199)
    blocks = fetch(args.rpc, lo, hi)
    hs = sorted(blocks)
    if len(hs) < 2:
        print("not enough blocks")
        return

    rows = []
    for i in range(len(hs) - 1):
        h = hs[i]
        t0, n = blocks[h]
        t1, _ = blocks[hs[i + 1]]
        rows.append((h, t0, n, (t1 - t0).total_seconds()))

    if args.csv:
        with open(args.csv, 'w') as f:
            f.write("height,time,num_txs,dt\n")
            for h, t, n, dt in rows:
                f.write(f"{h},{t.isoformat()},{n},{dt:.3f}\n")

    load = [r for r in rows if r[2] >= args.min_txs]
    if not load:
        print("no blocks matched --min-txs")
        return

    span = (load[-1][1] - load[0][1]).total_seconds()
    txs = sum(r[2] for r in load)
    # Wall-clock rate over the whole matched span, including any idle gaps.
    full_span = span + load[-1][3]
    print(f"blocks {load[0][0]}..{load[-1][0]}  n={len(load)}  span={full_span:.1f}s")
    print(f"txs={txs}  =>  {txs / full_span:,.0f} tx/s sustained")
    print()

    dts = sorted(r[3] for r in load)
    szs = sorted(r[2] for r in load)
    print("                min      p10      p50      p90      max     mean")
    print("block time  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f  %7.2f" % (
        dts[0], pct(dts, .1), pct(dts, .5), pct(dts, .9), dts[-1], sum(dts) / len(dts)))
    print("block txs   %7d  %7d  %7d  %7d  %7d  %7.0f" % (
        szs[0], pct(szs, .1), pct(szs, .5), pct(szs, .9), szs[-1], sum(szs) / len(szs)))
    print()

    on_time = sum(1 for d in dts if d <= 1.05)
    print(f"blocks within 1.05s: {on_time}/{len(dts)} ({100 * on_time / len(dts):.0f}%)")
    empty = [r for r in load if r[2] < 100]
    if empty:
        print(f"near-empty blocks (<100 tx): {len(empty)} ({100 * len(empty) / len(load):.0f}%), "
              f"{sum(r[3] for r in empty):.0f}s of {full_span:.0f}s wall time")

    # Marginal cost per tx: block time as a function of size. If the fit is
    # strong, the chain is pipeline-bound and bigger blocks buy nothing.
    print()
    print("size bucket        n   mean_dt  mean_txs   implied tx/s")
    for lo_b, hi_b in [(0, 100), (100, 2000), (2000, 5000), (5000, 8000),
                       (8000, 11000), (11000, 15000), (15000, 25000), (25000, 10 ** 9)]:
        sel = [r for r in load if lo_b <= r[2] < hi_b]
        if not sel:
            continue
        mdt = sum(r[3] for r in sel) / len(sel)
        mtx = sum(r[2] for r in sel) / len(sel)
        print("%6d-%-8d %4d  %7.2fs  %8.0f   %8.0f" % (
            lo_b, hi_b, len(sel), mdt, mtx, mtx / mdt if mdt else 0))

    big = [r for r in load if r[2] >= 2000]
    if len(big) >= 4:
        xs = [r[2] for r in big]
        ys = [r[3] for r in big]
        mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = sum((x - mx) ** 2 for x in xs)
        if den:
            slope = num / den
            icept = my - slope * mx
            print()
            print(f"fit over {len(big)} non-trivial blocks: block_time = {icept:.3f}s + txs/{1 / slope:,.0f}")
            print(f"  => marginal chain rate {1 / slope:,.0f} tx/s; "
                  f"at a 1.00s block that allows {max(0, (1.0 - icept) / slope):,.0f} txs/block")


if __name__ == '__main__':
    main()
