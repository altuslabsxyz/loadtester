package workload

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/stablelabs/loadtester/accounts"
)

// minSqrtRatioPlusOne returns MIN_SQRT_RATIO+1, the lowest valid sqrtPriceLimit
// for a zeroForOne swap (uniswap v3 TickMath.MIN_SQRT_RATIO = 4295128739).
func minSqrtRatioPlusOne() *big.Int { return big.NewInt(4295128740) }

// big1e24 is a large mint amount so per-tx transfers/swaps never run dry.
func big1e24() *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(24), nil)
}

// PrepareAccounts mints test tokens to every load account and approves the
// callee, so erc20-transfer and swap workloads do not revert for lack of
// balance/allowance. Sends are issued per account and waited on in bulk.
//
// TestERC20.mint is public, so each account self-mints. It mints:
//   - tokens[0] (the erc20-transfer lane token), if present
//   - pool.token0 (the swap input token), if a pool+callee exist
func (b *Builder) PrepareAccounts(ctx context.Context) error {
	feeCap, tip, err := b.pool.Fees(ctx)
	if err != nil {
		return fmt.Errorf("fees: %w", err)
	}

	// Collect the set of tokens that need minting, and whether to approve callee.
	type job struct {
		token   common.Address
		approve bool
	}
	var jobs []job
	if b.hasToken {
		jobs = append(jobs, job{token: b.token0, approve: false})
	}
	if b.hasPool && b.hasCallee {
		p, _ := b.dep.FirstPool()
		t0 := common.HexToAddress(p.Token0)
		if !(b.hasToken && t0 == b.token0) {
			jobs = append(jobs, job{token: t0, approve: true})
		} else {
			// same token; just ensure approval too
			jobs[0].approve = true
		}
	}
	if len(jobs) == 0 {
		return nil
	}

	callee := b.dep.CalleeAddr()
	amount := big1e24()
	maxUint := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

	var lastHash common.Hash
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, len(b.pool.Accs))

	for _, a := range b.pool.Accs {
		wg.Add(1)
		go func(a *accounts.Account) {
			defer wg.Done()
			for _, j := range jobs {
				// mint
				data, err := b.abis.PackMintERC20(a.Addr, amount)
				if err != nil {
					errs <- err
					return
				}
				tk := j.token
				tx, err := b.pool.SignStandard(a, a.Next(0), &tk, nil, data, 120000, feeCap, tip)
				if err != nil {
					errs <- err
					return
				}
				if err := b.pool.Client.SendTransaction(ctx, tx); err != nil {
					errs <- fmt.Errorf("mint send: %w", err)
					return
				}
				mu.Lock()
				lastHash = tx.Hash()
				mu.Unlock()

				if j.approve {
					adata, err := b.abis.PackApprove(callee, maxUint)
					if err != nil {
						errs <- err
						return
					}
					atx, err := b.pool.SignStandard(a, a.Next(0), &tk, nil, adata, 80000, feeCap, tip)
					if err != nil {
						errs <- err
						return
					}
					if err := b.pool.Client.SendTransaction(ctx, atx); err != nil {
						errs <- fmt.Errorf("approve send: %w", err)
						return
					}
					mu.Lock()
					lastHash = atx.Hash()
					mu.Unlock()
				}
			}
		}(a)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}

	// Wait for the last submitted setup tx to mine.
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r, err := b.pool.Client.TransactionReceipt(ctx, lastHash)
		if err == nil && r != nil {
			// A mint/approve tx is accepted into the mempool even when it will
			// revert on execution (e.g. the configured token is not self-mintable
			// on a real testnet). Checking the receipt STATUS prevents silently
			// proceeding with zero token balance - which would make every
			// erc20Transfer/swap send revert during the load phase.
			if r.Status != 1 {
				return fmt.Errorf("token setup tx %s reverted (status 0): the configured token is likely not "+
					"self-mintable on this chain - disable the erc20Transfer/swap workloads, or point deployment.json "+
					"at a mintable TestERC20", lastHash.Hex())
			}
			return nil
		}
		if sleep(ctx, 300*time.Millisecond) {
			return ctx.Err()
		}
	}
	return fmt.Errorf("token setup txs not mined in time")
}
