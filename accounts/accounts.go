// Package accounts manages the fan-out account pool: key generation, funding
// from a master key, per-(account,nonce-key) nonce tracking, and tx signing for
// both standard EVM txs and stable-geth 2D-nonce (VIP) txs.
package accounts

import (
	"context"
	"crypto/ecdsa"
	"fmt"
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

// Fund sends `amount` (whole gas tokens) from master to every load account and
// waits for the funding txs to be mined, then seeds each account's nonce.
func (p *Pool) Fund(ctx context.Context, amountWholeTokens string) error {
	amt, ok := new(big.Int).SetString(amountWholeTokens, 10)
	if !ok {
		return fmt.Errorf("invalid fund amount %q (expected integer whole-token string)", amountWholeTokens)
	}
	// whole token -> wei (18 decimals for native EVM balance)
	wei := new(big.Int).Mul(amt, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

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

	var lastHash common.Hash
	for _, a := range p.Accs {
		nonce := p.Master.Next(0)
		tx := types.NewTx(&types.DynamicFeeTx{
			ChainID:   p.ChainID,
			Nonce:     nonce,
			GasTipCap: tip,
			GasFeeCap: feeCap,
			Gas:       21000,
			To:        &a.Addr,
			Value:     wei,
		})
		signed, err := types.SignTx(tx, p.Signer, p.Master.Key)
		if err != nil {
			return fmt.Errorf("sign funding tx: %w", err)
		}
		if err := p.Client.SendTransaction(ctx, signed); err != nil {
			return fmt.Errorf("send funding tx to %s: %w", a.Addr, err)
		}
		lastHash = signed.Hash()
	}
	if err := p.waitMined(ctx, lastHash, 60*time.Second); err != nil {
		return fmt.Errorf("wait funding mined: %w", err)
	}
	for _, a := range p.Accs {
		if err := p.seedNonce(ctx, a); err != nil {
			return err
		}
	}
	return nil
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
// carries the VIP bit for the given lane id, routing it to that VIP lane.
func (p *Pool) SignVIP(a *Account, nonce uint64, laneID int32, to *common.Address, value *big.Int, data []byte, gas uint64, feeCap, tip *big.Int) (*types.Transaction, error) {
	if value == nil {
		value = big.NewInt(0)
	}
	nonceKey := stabletypes.VipFlag | uint64(laneID) // VIP bit set, lane id in lower 63 bits
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
