// Package accounts manages the fan-out account pool: key generation, funding
// from a master key, per-(account,nonce-key) nonce tracking, and tx signing for
// both standard EVM txs and stable-geth 2D-nonce (VIP) txs.
package accounts

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/holiman/uint256"

	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// Account is one signing identity with local 2D-nonce tracking.
type Account struct {
	Key  *ecdsa.PrivateKey
	Addr common.Address

	mu        sync.Mutex
	nonces    map[uint64]uint64 // nonceKey -> next sequence to assign
	confirmed map[uint64]uint64 // nonceKey -> on-chain confirmed (latest) nonce
}

// Next returns and increments the next sequence for the given nonce key.
// Convenience for sequential flows (funding/setup) that don't roll back.
func (a *Account) Next(nonceKey uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := a.nonces[nonceKey]
	a.nonces[nonceKey]++
	return n
}

// Peek returns the next sequence WITHOUT incrementing. Pair with Commit (only
// after a successful send) in the open-loop driver so a failed send does not
// burn a nonce - the same Peek value is simply reused next iteration.
func (a *Account) Peek(nonceKey uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nonces[nonceKey]
}

// Commit advances the next sequence after a successful send.
func (a *Account) Commit(nonceKey uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nonces[nonceKey]++
}

// SetBase seeds the next sequence for a nonce key (used for the standard key 0
// after reading the on-chain pending nonce).
func (a *Account) SetBase(nonceKey, seq uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nonces[nonceKey] = seq
	a.confirmed[nonceKey] = seq
}

// SetConfirmed records the latest on-chain confirmed nonce for a key.
func (a *Account) SetConfirmed(nonceKey, seq uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.confirmed[nonceKey] = seq
}

// Inflight returns assigned-minus-confirmed for a key (the open-loop window).
// Clamped at 0: confirmed can momentarily exceed the local assign pointer (poll
// races, external txs, re-seed), and an unsigned underflow would otherwise wrap
// to a huge value and silently disable the window.
func (a *Account) Inflight(nonceKey uint64) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.confirmed[nonceKey] >= a.nonces[nonceKey] {
		return 0
	}
	return int(a.nonces[nonceKey] - a.confirmed[nonceKey])
}

// Pool is the connected account pool bound to a chain.
type Pool struct {
	Client  *ethclient.Client
	ChainID *big.Int
	Signer  types.Signer
	Master  *Account
	Accs    []*Account
}

// parseKey accepts a hex private key with or without 0x.
func parseKey(hexKey string) (*ecdsa.PrivateKey, error) {
	return crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(hexKey), "0x"))
}

func newAccount(key *ecdsa.PrivateKey) *Account {
	return &Account{
		Key:       key,
		Addr:      crypto.PubkeyToAddress(key.PublicKey),
		nonces:    make(map[uint64]uint64),
		confirmed: make(map[uint64]uint64),
	}
}

// Connect dials the JSON-RPC endpoint and verifies the chain id matches the
// expected value. A mismatch aborts: signing with the wrong eip155 id would get
// every tx rejected.
func Connect(ctx context.Context, jsonrpc string, expectedChainID uint64) (*ethclient.Client, *big.Int, error) {
	c, err := ethclient.DialContext(ctx, jsonrpc)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", jsonrpc, err)
	}
	got, err := c.ChainID(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("eth_chainId on %s: %w", jsonrpc, err)
	}
	if got.Uint64() != expectedChainID {
		return nil, nil, fmt.Errorf("chain id mismatch: target says %d, node %s reports %d", expectedChainID, jsonrpc, got.Uint64())
	}
	return c, got, nil
}

// NewPool connects, loads the master key, and generates n load accounts.
func NewPool(ctx context.Context, jsonrpc, masterKeyHex string, n int, expectedChainID uint64) (*Pool, error) {
	c, chainID, err := Connect(ctx, jsonrpc, expectedChainID)
	if err != nil {
		return nil, err
	}
	mk, err := parseKey(masterKeyHex)
	if err != nil {
		return nil, fmt.Errorf("master key: %w", err)
	}
	p := &Pool{
		Client:  c,
		ChainID: chainID,
		Signer:  types.LatestSignerForChainID(chainID),
		Master:  newAccount(mk),
		Accs:    make([]*Account, 0, n),
	}
	for i := 0; i < n; i++ {
		k, err := crypto.GenerateKey()
		if err != nil {
			return nil, fmt.Errorf("generate account %d: %w", i, err)
		}
		p.Accs = append(p.Accs, newAccount(k))
	}
	// Seed the master's standard nonce from chain.
	if err := p.seedNonce(ctx, p.Master); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Pool) seedNonce(ctx context.Context, a *Account) error {
	n, err := p.Client.PendingNonceAt(ctx, a.Addr)
	if err != nil {
		return fmt.Errorf("pending nonce for %s: %w", a.Addr, err)
	}
	a.SetBase(0, n)
	return nil
}

// Fees returns (gasFeeCap, gasTipCap) from the current base fee. Callers on the
// hot path should fetch once and reuse, passing the values to the Sign* helpers.
func (p *Pool) Fees(ctx context.Context) (*big.Int, *big.Int, error) {
	head, err := p.Client.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	tip, err := p.Client.SuggestGasTipCap(ctx)
	if err != nil || tip == nil {
		tip = big.NewInt(0)
	}
	base := head.BaseFee
	if base == nil {
		base = big.NewInt(0)
	}
	// feeCap = 2*base + tip
	feeCap := new(big.Int).Mul(base, big.NewInt(2))
	feeCap.Add(feeCap, tip)
	return feeCap, tip, nil
}

// parseWholeTokensToWei converts a DECIMAL whole-token string to wei (×1e18),
// exactly (no binary-float error). Accepts "1", "0.01", "2.5", etc. Fractions
// beyond 18 decimals are truncated (sub-wei). Rejects negative/invalid input.
// This lets accounts be funded with only the tiny amount they need for gas
// (~0.00002/tx) instead of a full token, so most of the float isn't tied up.
func parseWholeTokensToWei(s string) (*big.Int, error) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "+"))
	if s == "" {
		return nil, fmt.Errorf("empty fund amount (set funding.fundPerAccount, e.g. \"0.01\")")
	}
	if strings.HasPrefix(s, "-") {
		return nil, fmt.Errorf("fund amount %q must not be negative", s)
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	if intPart == "" {
		intPart = "0"
	}
	if len(fracPart) > 18 { // truncate below 1 wei
		fracPart = fracPart[:18]
	}
	fracPart += strings.Repeat("0", 18-len(fracPart)) // pad to 18 decimals
	wei, ok := new(big.Int).SetString(intPart+fracPart, 10)
	if !ok {
		return nil, fmt.Errorf("invalid fund amount %q (expected a decimal whole-token amount, e.g. \"0.01\")", s)
	}
	return wei, nil
}

// fundPair is one transfer in the fan-out funding schedule: sender -1 is the
// master, any other index is a load account funded in an earlier round.
type fundPair struct {
	sender   int
	receiver int
}

// fundSchedule builds the fan-out funding rounds for n accounts. The chain
// admits only nonce == committed nonce per sender (no future-nonce queue), so
// one sender can fund at most one account per block. Instead of the master
// funding all n in lockstep (~n blocks), each round the master AND every
// already-funded account fund one new account each, so the funded set roughly
// doubles per round: ceil(log2(n+1)) rounds total.
func fundSchedule(n int) [][]fundPair {
	var rounds [][]fundPair
	funded := 0
	for funded < n {
		senders := funded + 1 // master + everyone funded in earlier rounds
		var round []fundPair
		for s := 0; s < senders && funded < n; s++ {
			round = append(round, fundPair{sender: s - 1, receiver: funded})
			funded++
		}
		rounds = append(rounds, round)
	}
	return rounds
}

// fundEndowments computes how much each account must RECEIVE: its own
// perAccount amount plus, for every account it later funds, that child's
// endowment and one tx of gas. Receivers always have a higher index than their
// sender and only send in later rounds, so walking rounds in reverse resolves
// children before their parents. Total master outflow stays exactly
// n*(perAccount+gasPerTx) - the same float lockstep funding needed.
func fundEndowments(rounds [][]fundPair, n int, perAccount, gasPerTx *big.Int) []*big.Int {
	endow := make([]*big.Int, n)
	for i := range endow {
		endow[i] = new(big.Int).Set(perAccount)
	}
	for r := len(rounds) - 1; r >= 0; r-- {
		for _, fp := range rounds[r] {
			if fp.sender >= 0 {
				endow[fp.sender].Add(endow[fp.sender], endow[fp.receiver])
				endow[fp.sender].Add(endow[fp.sender], gasPerTx)
			}
		}
	}
	return endow
}

// Fund distributes `amount` (decimal whole gas tokens, e.g. "0.01") to every
// load account via the fan-out tree (see fundSchedule), waiting for each
// round's txs to mine before the next, then seeds each account's nonce.
func (p *Pool) Fund(ctx context.Context, amountWholeTokens string) error {
	wei, err := parseWholeTokensToWei(amountWholeTokens)
	if err != nil {
		return err
	}
	if wei.Sign() <= 0 {
		return fmt.Errorf("fund amount %q must be > 0", amountWholeTokens)
	}

	feeCap, tip, err := p.Fees(ctx)
	if err != nil {
		return fmt.Errorf("suggest fees: %w", err)
	}

	// Master-balance precheck: on a public testnet a faucet-limited master that
	// can't cover N transfers + gas would otherwise fund an arbitrary prefix of
	// accounts and then fail mid-stream (or time out waiting), leaving the rest
	// unfunded. Fail fast with a clear, actionable message instead.
	n := int64(len(p.Accs))
	bal, err := p.Client.BalanceAt(ctx, p.Master.Addr, nil)
	if err != nil {
		return fmt.Errorf("read master balance: %w", err)
	}
	need := new(big.Int).Mul(wei, big.NewInt(n))
	gasPerTx := new(big.Int).Mul(big.NewInt(21000), feeCap)
	need.Add(need, new(big.Int).Mul(gasPerTx, big.NewInt(n)))
	if bal.Cmp(need) < 0 {
		return fmt.Errorf("master %s balance %s wei < required ~%s wei (%d accounts x %s wei + gas); "+
			"fund the master or lower funding.accountsN / fundPerAccount", p.Master.Addr.Hex(), bal, need, n, wei)
	}

	// Fan-out funding: within a round every tx has a DISTINCT sender, so the
	// 1-in-flight-per-sender rule (nonce gaps are rejected, not queued) is never
	// violated and the whole round lands in ~one block. Rounds are serialized:
	// a round's receivers become the next round's senders, so their funding txs
	// must be committed first.
	//
	// Broadcasts within a round are SEQUENTIAL, receipts are awaited in
	// parallel. Concurrent eth_sendRawTransaction can panic the node's CheckTx
	// when two executions race to create a not-yet-existing account in the fee
	// path (x/auth "index uniqueness constrain violation", recovered panic, tx
	// rejected). A broadcast is ~milliseconds, so serializing them costs almost
	// nothing; the round still mines in ~one block.
	rounds := fundSchedule(len(p.Accs))
	endow := fundEndowments(rounds, len(p.Accs), wei, gasPerTx)
	funded := 0
	for ri, round := range rounds {
		type sent struct {
			hash common.Hash
			addr common.Address
		}
		txs := make([]sent, 0, len(round))
		for _, fp := range round {
			sender := p.Master
			if fp.sender >= 0 {
				sender = p.Accs[fp.sender]
			}
			recv := p.Accs[fp.receiver]
			tx := types.NewTx(&types.DynamicFeeTx{
				ChainID:   p.ChainID,
				Nonce:     sender.Next(0),
				GasTipCap: tip,
				GasFeeCap: feeCap,
				Gas:       21000,
				To:        &recv.Addr,
				Value:     endow[fp.receiver],
			})
			signed, err := types.SignTx(tx, p.Signer, sender.Key)
			if err != nil {
				return fmt.Errorf("sign funding tx for %s: %w", recv.Addr, err)
			}
			if err := p.sendWithRetry(ctx, signed); err != nil {
				return fmt.Errorf("send funding tx to %s: %w", recv.Addr, err)
			}
			txs = append(txs, sent{hash: signed.Hash(), addr: recv.Addr})
		}
		var wg sync.WaitGroup
		errCh := make(chan error, len(txs))
		for _, s := range txs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := p.waitMined(ctx, s.hash, 60*time.Second); err != nil {
					errCh <- fmt.Errorf("wait funding tx to %s: %w", s.addr, err)
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for e := range errCh {
			if e != nil {
				return e
			}
		}
		funded += len(round)
		log.Printf("[fund] round %d/%d mined: %d/%d accounts funded", ri+1, len(rounds), funded, len(p.Accs))
	}

	// Re-seed every account's nonce from chain: tree senders consumed nonces.
	var wg sync.WaitGroup
	errCh := make(chan error, len(p.Accs))
	for _, a := range p.Accs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.seedNonce(ctx, a); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
}

// sweepAmount returns how much of `bal` can be sent back to the master after
// reserving `gasReserve` for the sweep tx itself. Returns nil if the balance
// can't even cover the reserve (nothing worth sweeping).
func sweepAmount(bal, gasReserve *big.Int) *big.Int {
	amount := new(big.Int).Sub(bal, gasReserve)
	if amount.Sign() <= 0 {
		return nil
	}
	return amount
}

// Sweep returns each load account's leftover native balance to the master,
// reserving one tx of gas per account. This recovers the funds Fund sent out:
// the load-account keys are random and in-memory only, so anything left in them
// is unrecoverable once the process exits. Best-effort - an account that can't
// cover its own sweep gas, or whose send/mine fails, is skipped and logged; the
// total amount recovered (in wei) is returned. Each account is a distinct
// sender with a single tx, so the 1-in-flight-per-sender rule allows all sweeps
// to run concurrently (~one block total instead of one block per account).
func (p *Pool) Sweep(ctx context.Context) (*big.Int, error) {
	feeCap, tip, err := p.Fees(ctx)
	if err != nil {
		return nil, fmt.Errorf("suggest fees: %w", err)
	}
	// Max cost of one 21000-gas transfer at this feeCap. The EIP-1559 balance
	// check requires balance >= value + gas*feeCap, so reserving exactly this
	// lets the tx pass while sending everything else back.
	gasReserve := new(big.Int).Mul(big.NewInt(21000), feeCap)

	recovered := new(big.Int)
	swept, skipped := 0, 0
	var mu sync.Mutex
	// Serializes broadcasts across the sweep goroutines: concurrent
	// eth_sendRawTransaction can panic the node's CheckTx (see Fund). Reads,
	// signing, and receipt waits stay fully concurrent.
	var sendMu sync.Mutex
	var wg sync.WaitGroup
	for i, a := range p.Accs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			skip := func(format string, args ...any) {
				log.Printf("[sweep] %d/%d: "+format, append([]any{i + 1, len(p.Accs)}, args...)...)
				mu.Lock()
				skipped++
				mu.Unlock()
			}
			bal, err := p.Client.BalanceAt(ctx, a.Addr, nil)
			if err != nil {
				skip("balance read failed for %s: %v (skipped)", a.Addr, err)
				return
			}
			amount := sweepAmount(bal, gasReserve)
			if amount == nil {
				mu.Lock()
				skipped++ // dust: not enough to cover the sweep's own gas
				mu.Unlock()
				return
			}
			nonce, err := p.Client.PendingNonceAt(ctx, a.Addr)
			if err != nil {
				skip("nonce read failed for %s: %v (skipped)", a.Addr, err)
				return
			}
			tx := types.NewTx(&types.DynamicFeeTx{
				ChainID:   p.ChainID,
				Nonce:     nonce,
				GasTipCap: tip,
				GasFeeCap: feeCap,
				Gas:       21000, // intrinsic gas for a value transfer to an EOA (master is an EOA, same as Fund assumes)
				To:        &p.Master.Addr,
				Value:     amount,
			})
			signed, err := types.SignTx(tx, p.Signer, a.Key)
			if err != nil {
				skip("sign failed for %s: %v (skipped)", a.Addr, err)
				return
			}
			sendMu.Lock()
			err = p.sendWithRetry(ctx, signed)
			sendMu.Unlock()
			if err != nil {
				skip("send failed for %s: %v (skipped)", a.Addr, err)
				return
			}
			if err := p.waitMined(ctx, signed.Hash(), 60*time.Second); err != nil {
				skip("not mined for %s: %v (funds may still return)", a.Addr, err)
				return
			}
			mu.Lock()
			recovered.Add(recovered, amount)
			swept++
			mu.Unlock()
		}()
	}
	wg.Wait()
	log.Printf("[sweep] recovered %s wei to master from %d/%d accounts (%d skipped)",
		recovered, swept, len(p.Accs), skipped)
	return recovered, nil
}

// sendWithRetry broadcasts a signed tx, retrying transient rejections with a
// growing backoff. Retries resend the SAME signed tx, so they are idempotent -
// a duplicate that already reached the pool comes back "already known" and
// counts as success. This covers the node's recovered CheckTx panics (e.g. the
// account-creation index-conflict race): the state that caused the race
// commits within a block, after which the resend is admitted.
func (p *Pool) sendWithRetry(ctx context.Context, tx *types.Transaction) error {
	const attempts = 5
	var err error
	for i := 1; ; i++ {
		err = p.Client.SendTransaction(ctx, tx)
		if err == nil {
			return nil
		}
		if strings.Contains(err.Error(), "already known") {
			return nil // an earlier attempt landed in the pool
		}
		if i == attempts {
			return err
		}
		msg := err.Error()
		if len(msg) > 200 { // node errors can embed a full panic stack
			msg = msg[:200] + "..."
		}
		log.Printf("[send] broadcast attempt %d/%d failed (%s) - retrying", i, attempts, msg)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(i) * 500 * time.Millisecond):
		}
	}
}

func (p *Pool) waitMined(ctx context.Context, hash common.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r, err := p.Client.TransactionReceipt(ctx, hash)
		if err == nil && r != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return fmt.Errorf("tx %s not mined within %s", hash.Hex(), timeout)
}

// SignStandard builds and signs a standard DynamicFee EVM tx (nonce key 0) with
// an explicit nonce and caller-provided fees. The caller owns nonce assignment
// (Peek/Commit/Rollback) so a failed send does not burn a nonce.
func (p *Pool) SignStandard(a *Account, nonce uint64, to *common.Address, value *big.Int, data []byte, gas uint64, feeCap, tip *big.Int) (*types.Transaction, error) {
	if value == nil {
		value = big.NewInt(0)
	}
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   p.ChainID,
		Nonce:     nonce,
		GasTipCap: tip,
		GasFeeCap: feeCap,
		Gas:       gas,
		To:        to,
		Value:     value,
		Data:      data,
	})
	return types.SignTx(tx, p.Signer, a.Key)
}

// SignVIP builds and signs a stable-geth 2D-nonce (CustomTx) tx whose nonce key
// carries the Enterprise bit for the given lane ID. The method retains its
// legacy name because target YAML and workload kinds still use "vip".
func (p *Pool) SignVIP(a *Account, nonce uint64, laneID int32, to *common.Address, value *big.Int, data []byte, gas uint64, feeCap, tip *big.Int) (*types.Transaction, error) {
	if laneID < 0 {
		return nil, fmt.Errorf("enterprise lane ID must be non-negative: %d", laneID)
	}
	if value == nil {
		value = big.NewInt(0)
	}
	nonceKey := stabletypes.EnterpriseFlag | uint64(laneID)
	chainID, _ := uint256.FromBig(p.ChainID)
	val, _ := uint256.FromBig(value)
	feeCapU, _ := uint256.FromBig(feeCap)
	tipU, _ := uint256.FromBig(tip)
	tx := types.NewTx(&types.CustomTx{
		ChainID:          chainID,
		Nonce:            nonce,
		GasTipCap:        tipU,
		GasFeeCap:        feeCapU,
		Gas:              gas,
		To:               to,
		Value:            val,
		Data:             data,
		NonceKey:         nonceKey,
		TimeoutTimestamp: uint256.NewInt(0), // ordered (not an unordered/timeout tx)
	})
	return types.SignTx(tx, p.Signer, a.Key)
}

// SignUnordered builds and signs a stable-geth unordered (2D-nonce) tx:
// NonceKey = MaxUint64 (the unordered marker), Nonce MUST be 0, and
// TimeoutTimestamp is a Unix-NANOSECOND deadline. The chain dedupes unordered
// txs by (sender, timeout), so callers must pass a UNIQUE future timeout within
// the chain's max TTL (~10m). Exercises the selective-recheck eviction path.
func (p *Pool) SignUnordered(a *Account, to *common.Address, value *big.Int, data []byte, gas uint64, feeCap, tip *big.Int, timeoutUnixNano int64) (*types.Transaction, error) {
	if value == nil {
		value = big.NewInt(0)
	}
	chainID, _ := uint256.FromBig(p.ChainID)
	val, _ := uint256.FromBig(value)
	feeCapU, _ := uint256.FromBig(feeCap)
	tipU, _ := uint256.FromBig(tip)
	tx := types.NewTx(&types.CustomTx{
		ChainID:          chainID,
		Nonce:            0, // required to be 0 for unordered txs
		GasTipCap:        tipU,
		GasFeeCap:        feeCapU,
		Gas:              gas,
		To:               to,
		Value:            val,
		Data:             data,
		NonceKey:         math.MaxUint64,
		TimeoutTimestamp: new(uint256.Int).SetUint64(uint64(timeoutUnixNano)),
	})
	return types.SignTx(tx, p.Signer, a.Key)
}
