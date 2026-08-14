//go:build ignore

// depthprobe answers the one question the send engine is built around:
//
//	can a single account hold MORE THAN ONE ordered tx in the mempool at a time?
//
// engine.go asserts it cannot ("the chain admits at most ONE ordered tx per
// (sender, nonce-key) - a future nonce is rejected, not queued") and derives its
// whole design from that: an account with a tx in flight leaves the ready queue
// until the send endpoint COMMITS a block containing it. With depth 1, Little's
// law makes the achievable rate exactly accountsN/releaseLatency - and on this
// devnet the send endpoint is the tx-provider full node, which commits 3-4s
// behind the validators, so most of that latency is pure waiting.
//
// The assertion is only half right. ante/evm/09_increment_sequence.go rejects
// txNonce > accountNonce, but it reads accountNonce from the CheckTx state and
// WRITES back nonce+1 there, and that state persists across CheckTx calls until
// the next Commit. So nonce N+1 arriving behind an admitted nonce N should see
// accountNonce == N+1 and pass. This probe measures which is true.
//
// Usage:
//
//	go run scripts/depthprobe.go -rpc http://10.10.30.15:8545 \
//	  -seed 0xabab... -chainid 988 -accounts 20 -depth 8
//
// Reads only: it sends `depth` transfers of 1 wei per test account and lets them
// mine. Report is per-depth admission counts plus what actually committed.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/stablelabs/loadtester/accounts"
)

func main() {
	rpc := flag.String("rpc", "http://10.10.30.15:8545", "JSON-RPC endpoint (the SEND endpoint)")
	seed := flag.String("seed", "", "accountSeed hex from the target yaml")
	// No default: this is a private key, and .gitignore keeps every file that
	// carries one out of the repo. Pass funding.masterKey from your target yaml.
	master := flag.String("master", "", "funding.masterKey from the target yaml (only used to construct the pool)")
	chainID := flag.Uint64("chainid", 988, "EVM chain id")
	nAcc := flag.Int("accounts", 20, "how many pool accounts to probe")
	depth := flag.Int("depth", 8, "how many consecutive nonces to fire per account")
	offset := flag.Int("offset", 0, "first pool index to use")
	settle := flag.Duration("settle", 20*time.Second, "how long to wait for the sent txs to commit")
	gap := flag.Duration("gap", 0, "pause between an account's consecutive nonces")
	flag.Parse()

	if *seed == "" {
		log.Fatal("-seed is required (funding.accountSeed from the target yaml)")
	}
	if *master == "" {
		log.Fatal("-master is required (funding.masterKey from the target yaml)")
	}
	ctx := context.Background()

	pool, err := accounts.NewPoolWithOptions(ctx, *rpc, *master, *offset+*nAcc, *chainID, accounts.PoolOptions{AccountSeed: *seed})
	if err != nil {
		log.Fatalf("pool: %v", err)
	}
	feeCap, tip, err := pool.Fees(ctx)
	if err != nil {
		log.Fatalf("fees: %v", err)
	}
	log.Printf("[probe] endpoint=%s accounts=%d depth=%d feeCap=%s tip=%s",
		*rpc, *nAcc, *depth, feeCap, tip)

	accs := pool.Accs[*offset:]
	dead := common.HexToAddress("0x000000000000000000000000000000000000dEaD")

	type result struct {
		idx      int
		addr     common.Address
		start    uint64
		verdicts []string // one per depth slot
		final    uint64
	}
	results := make([]result, len(accs))

	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i, a := range accs {
		wg.Add(1)
		go func(i int, a *accounts.Account) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			n0, err := accounts.CommittedNonceAt(ctx, pool.Client, a.Addr)
			if err != nil {
				results[i] = result{idx: i, addr: a.Addr, verdicts: []string{"nonce read failed: " + err.Error()}}
				return
			}
			r := result{idx: i, addr: a.Addr, start: n0}
			// Fire consecutive nonces back to back on ONE connection, in order.
			// No waiting between them: the whole point is whether N+1 is
			// admissible while N is still only in the mempool.
			for d := 0; d < *depth; d++ {
				// -gap separates the nonces in time. It is the whole experiment
				// for a load driver: bursting a run inside one CheckTx epoch is
				// not the same as sending nonce N+1 seconds after N, once
				// intervening Commits have reset the app's CheckTx nonce for
				// every sender the block did not touch.
				if d > 0 && *gap > 0 {
					time.Sleep(*gap)
				}
				tx, err := pool.SignStandard(a, n0+uint64(d), &dead, big.NewInt(1), nil, 21000, feeCap, tip)
				if err != nil {
					r.verdicts = append(r.verdicts, "build: "+err.Error())
					break
				}
				if err := pool.Client.SendTransaction(ctx, tx); err != nil {
					r.verdicts = append(r.verdicts, errKey(err.Error()))
					continue
				}
				r.verdicts = append(r.verdicts, "accepted")
			}
			results[i] = r
		}(i, a)
	}
	wg.Wait()

	// Per-depth admission tally: slot d is "how many accounts had their d-th
	// consecutive nonce admitted". Slot 0 accepted / slot 1 rejected == depth 1.
	log.Printf("[probe] all sends issued; per-depth admission:")
	for d := 0; d < *depth; d++ {
		tally := map[string]int{}
		for _, r := range results {
			if d < len(r.verdicts) {
				tally[r.verdicts[d]]++
			}
		}
		fmt.Printf("  nonce+%d: %s\n", d, render(tally))
	}

	log.Printf("[probe] waiting %s for commits...", *settle)
	time.Sleep(*settle)

	advanced := map[uint64]int{}
	for i, a := range accs {
		n, err := accounts.CommittedNonceAt(ctx, pool.Client, a.Addr)
		if err != nil {
			continue
		}
		results[i].final = n
		advanced[n-results[i].start]++
	}
	fmt.Println()
	log.Printf("[probe] committed nonce advance per account (how many of the %d actually mined):", *depth)
	keys := make([]uint64, 0, len(advanced))
	for k := range advanced {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, k := range keys {
		fmt.Printf("  +%d nonces: %d accounts\n", k, advanced[k])
	}

	maxAdv := uint64(0)
	for _, k := range keys {
		if k > maxAdv {
			maxAdv = k
		}
	}
	fmt.Println()
	if maxAdv > 1 {
		log.Printf("[probe] VERDICT: per-account in-flight depth is > 1 (max observed %d). "+
			"engine.go's depth-1 assumption is WRONG and costs throughput.", maxAdv)
	} else {
		log.Printf("[probe] VERDICT: per-account in-flight depth is 1. engine.go's assumption holds; "+
			"accountsN is the only rate dial.")
	}
}

// errKey collapses a node error to a short stable label.
func errKey(msg string) string {
	for _, k := range []string{
		"nonce is higher than account nonce", "nonce is lower than account nonce",
		"nonce gap", "nonce too high", "nonce too low",
		"already known", "already in mempool", "replacement",
		"mempool is full", "insufficient funds",
	} {
		if contains(msg, k) {
			return k
		}
	}
	if len(msg) > 90 {
		return msg[:90]
	}
	return msg
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func render(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		if out != "" {
			out += "  "
		}
		out += fmt.Sprintf("%s=%d", k, m[k])
	}
	return out
}
