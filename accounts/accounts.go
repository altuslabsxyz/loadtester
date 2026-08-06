// Package accounts manages the fan-out account pool: key generation, funding
// from a master key, per-(account,nonce-key) nonce tracking, and tx signing for
// both standard EVM txs and stable-geth 2D-nonce (VIP) txs.
package accounts

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
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
	locked    map[uint64]bool   // nonceKey -> exclusive signer slot held
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

// TryAcquire reserves the account/nonce-key pair for an exclusive caller.
func (a *Account) TryAcquire(nonceKey uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.locked[nonceKey] {
		return false
	}
	a.locked[nonceKey] = true
	return true
}

// Release clears a previous TryAcquire reservation for the account/nonce-key.
func (a *Account) Release(nonceKey uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.locked, nonceKey)
}

// HasBase reports whether a nonce base has been seeded for the key (via
// SetBase). Zero is a valid nonce, so presence - not value - is the signal.
func (a *Account) HasBase(nonceKey uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.confirmed[nonceKey]
	return ok
}

// SetBase seeds the next sequence for a nonce key (used for the standard key 0
// after reading the on-chain pending nonce).
func (a *Account) SetBase(nonceKey, seq uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nonces[nonceKey] = seq
	a.confirmed[nonceKey] = seq
}

// Pool is the connected account pool bound to a chain.
type Pool struct {
	Client  *ethclient.Client
	ChainID *big.Int
	Signer  types.Signer
	Master  *Account
	Accs    []*Account

	// lastStates caches the most recent loadAccountStates result. Fund ends by
	// re-reading every account, so a caller that only needs post-funding
	// balances (the spend preflight) can reuse that instead of paying for
	// another 2-RPC-per-account sweep of the whole pool.
	statesMu   sync.Mutex
	lastStates []accountChainState
}

// PoolOptions controls optional account-pool persistence. Empty options keep
// the legacy behavior: crypto-random ephemeral load accounts.
type PoolOptions struct {
	AccountSeed  string
	AccountsFile string
}

const (
	minAccountSeedBytes = 32
	accountDeriveDomain = "loadtester/account-derivation/v1"
	accountFingerDomain = "loadtester/account-fingerprint/v1"
	manifestVersion     = 1
	// fundingWorkers bounds concurrent balance/nonce reads and receipt polls
	// during funding. These are read-only RPCs, never the serialized-broadcast
	// hazard, so the old value of 32 bought nothing: a 36000-account pool is
	// 72000 state reads per pass and loadAccountStates runs twice per Fund, so
	// 32 workers cost ~45s of pure waiting. 128 cuts that to ~11s and is still
	// far below what the load phase puts on the same endpoint.
	fundingWorkers = 128
	sweepWorkers   = 32
	// fundingBroadcastWorkers bounds CONCURRENT funding broadcasts within one
	// round (see sendFundingBatch). Broadcasts, not receipts, dominate a funding
	// pass: 21000 transfers at ~10ms each is ~3.5 minutes serial and ~1.6s here.
	// 128 is well under the concurrency the load phase itself puts on the same
	// endpoint (workload.workers is 512-1536), so it adds no new peak load.
	fundingBroadcastWorkers = 128
	// queryRetryAttempts/queryRetryBase bound the retry of a transient state
	// read (linear backoff: 150ms, 300ms, ... ~5.4s total).
	queryRetryAttempts = 8
	queryRetryBase     = 150 * time.Millisecond
)

// isTransientQueryErr reports whether a failed state READ can succeed on retry.
//
// The chain advances CometBFT's `latest height` at commit time, but the memiavl
// version for that height only becomes queryable a moment later. A state query
// landing inside that window is refused outright:
//
//	failed to load state at height 33247821; historical version not ready:
//	33247821: invalid height (latest height: 33247821)
//
// That is a commit race, not a real error - the identical query succeeds
// milliseconds later. Setup reads 2 values per account (balance + nonce), so at
// accountsN in the thousands hitting the window is near-certain, and a single
// unretried hit used to abort the entire run in the funding phase.
//
// Transport-level failures are included: the read never reached a verdict, and
// reads are idempotent so replaying one is always safe. Definitive rejections
// (bad address, unknown method) do not match and still fail fast.
func isTransientQueryErr(err error) bool {
	if err == nil {
		return false
	}
	// The CALLER's deadline/cancellation - retrying cannot help and would only
	// mask shutdown.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		// memiavl / versiondb commit race
		"historical version not ready",
		"failed to load state at height",
		"invalid height",
		"version does not exist",
		"proof queries at height",
		// node briefly out of capacity or restarting
		"resource exhausted", "unavailable", "server is not up",
		// transport: request may never have been evaluated
		"connection reset", "connection refused", "broken pipe",
		"unexpected eof", "eof", "i/o timeout", "timeout awaiting",
		"too many open files", "no such host", "server misbehaving",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// retryQuery runs an idempotent chain read, retrying transient failures with a
// bounded linear backoff. Every state read in this tool should go through it: a
// bare client call is one memiavl commit race away from aborting a run.
func retryQuery[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	var err error
	for i := 1; i <= queryRetryAttempts; i++ {
		var v T
		if v, err = fn(ctx); err == nil {
			return v, nil
		}
		if !isTransientQueryErr(err) || i == queryRetryAttempts {
			return zero, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(time.Duration(i) * queryRetryBase):
		}
	}
	return zero, err
}

// PendingNonceAt reads the account's next nonce, retrying the commit race.
func PendingNonceAt(ctx context.Context, c *ethclient.Client, addr common.Address) (uint64, error) {
	return retryQuery(ctx, func(ctx context.Context) (uint64, error) {
		return c.PendingNonceAt(ctx, addr)
	})
}

// CommittedNonceAt reads the account's nonce at the latest COMMITTED block,
// retrying the commit race. This is the engine's admission probe.
func CommittedNonceAt(ctx context.Context, c *ethclient.Client, addr common.Address) (uint64, error) {
	return retryQuery(ctx, func(ctx context.Context) (uint64, error) {
		return c.NonceAt(ctx, addr, nil)
	})
}

// BalanceAt reads the account's native balance, retrying the commit race.
func BalanceAt(ctx context.Context, c *ethclient.Client, addr common.Address) (*big.Int, error) {
	return retryQuery(ctx, func(ctx context.Context) (*big.Int, error) {
		return c.BalanceAt(ctx, addr, nil)
	})
}

// isNonceMismatchErr reports whether a broadcast was refused because the tx's
// nonce does not match the sender's account nonce. Resending the SAME signed
// bytes can never fix this - the nonce must be re-read and the tx re-signed.
//
// The stable ante surfaces these verbatim through eth_sendRawTransaction, e.g.
// "got 58, expected 59: txnonce is lower than account nonce".
func isNonceMismatchErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"nonce is lower than account nonce", "nonce is higher than account nonce",
		"nonce too low", "nonce too high", "nonce gap", "invalid nonce",
		"invalid sequence", "sequence mismatch", "account sequence mismatch",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// isAlreadyKnownErr reports whether the node already holds this exact tx, which
// means an earlier (possibly ambiguous) attempt was admitted - a success.
func isAlreadyKnownErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"already known", "already in mempool", "already exists in cache", "known transaction",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// isSlotOccupiedErr reports whether a DIFFERENT tx already holds this
// (sender, nonce) in the node's app-side mempool and ours did not out-bid it.
//
//	tx doesn't fit the replacement rule, oldPriority: 125, newPriority: 125
//
// Seen when a previous run's load tx survives in the app mempool after CometBFT's
// CList has already drained to 0: the slot stays blocked, so a funding tx priced
// identically (both derive fees from the same base fee) is refused every time.
// The only way through is to price strictly above the resident tx.
func isSlotOccupiedErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"replacement rule", "replacement transaction", "oldpriority",
		"underpriced", "fee too low",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// errSlotWedged marks a (sender, nonce) slot that this chain cannot free.
//
// The error text advertises a priority comparison ("oldPriority: 125,
// newPriority: 784"), which reads like an RBF rule that a fee bump could beat.
// It cannot: app/mempool.go sets TxReplacement to a function that
// unconditionally returns false, so replacement is disabled outright and the
// priorities in the message are cosmetic. A re-send at 6x the tip is refused
// exactly like one at par (measured).
//
// The resident tx can therefore only leave by being included or evicted. When it
// is stuck in the app-side mempool while CometBFT's CList already reports zero
// unconfirmed txs, nothing will ever move it, and that account's nonce is
// wedged until the node restarts. Funding must route around such an account
// rather than abort a 6000-account pass over one of them.
var errSlotWedged = errors.New("nonce slot held by a stale tx and replacement is disabled chain-side")

// errSenderBroke marks a funding-tree sender that cannot pay for its transfer.
//
// A tree sender is only ever broke because its OWN top-up was skipped earlier in
// the pass (see errSlotWedged): the schedule is precomputed, so a receiver that
// never got funded is still scheduled to fund others in later rounds. Aborting
// there would turn one wedged account into a failed run on a fresh pool, so this
// is skipped and counted exactly like a wedged slot.
var errSenderBroke = errors.New("funding-tree sender has insufficient funds (its own top-up was skipped)")

// isBrokeErr reports whether a broadcast was refused for lack of funds.
func isBrokeErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "insufficient funds") || strings.Contains(msg, "insufficient balance")
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
		locked:    make(map[uint64]bool),
	}
}

// NewAccount wraps a private key as a load Account with empty nonce state.
// For offline flows (tests, tooling); networked pools come from NewPool.
func NewAccount(key *ecdsa.PrivateKey) *Account { return newAccount(key) }

// sharedEVMHTTPClient is reused across every EVM JSON-RPC dial so the many
// bounded sender workers REUSE a small pool of keep-alive connections
// instead of opening (and tearing down) a fresh TCP+TLS connection per call.
//
// Go's default HTTP transport keeps only MaxIdleConnsPerHost=2 connections idle,
// so under high accountsN every concurrent JSON-RPC call beyond the second
// opened a brand-new connection and closed it right after completing. That
// connection churn - thousands of short-lived connections per second - piles up
// TIME_WAIT sockets and netfilter conntrack entries on the *target* node and
// forces a full TLS handshake per call, saturating CPU. Once conntrack fills the
// kernel drops new packets (SSH included), which is what made the fullnode host
// unreachable. Raising the idle-conn ceiling lets connections be reused, so the
// steady-state connection count settles at ~the number of concurrent senders and
// churn collapses to near zero. MaxConnsPerHost is intentionally left unset
// (0/unlimited) so this change only affects connection REUSE, not throughput.
var sharedEVMHTTPClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          2048,
		MaxIdleConnsPerHost:   2048, // default is 2 - the root cause of the connection churn
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

// dialEVM dials an EVM JSON-RPC endpoint. For http(s) endpoints it routes the
// RPC client through the shared connection-pooling HTTP client above; other
// schemes (ws/wss/ipc) are not connection-per-call and fall back to the default
// dialer.
func dialEVM(ctx context.Context, rawurl string) (*ethclient.Client, error) {
	if u, err := url.Parse(rawurl); err == nil && (u.Scheme == "http" || u.Scheme == "https") {
		rc, err := rpc.DialOptions(ctx, rawurl, rpc.WithHTTPClient(sharedEVMHTTPClient))
		if err != nil {
			return nil, err
		}
		return ethclient.NewClient(rc), nil
	}
	return ethclient.DialContext(ctx, rawurl)
}

// Connect dials the JSON-RPC endpoint and verifies the chain id matches the
// expected value. A mismatch aborts: signing with the wrong eip155 id would get
// every tx rejected.
func Connect(ctx context.Context, jsonrpc string, expectedChainID uint64) (*ethclient.Client, *big.Int, error) {
	c, err := dialEVM(ctx, jsonrpc)
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
	return NewPoolWithOptions(ctx, jsonrpc, masterKeyHex, n, expectedChainID, PoolOptions{})
}

// NewPoolWithOptions connects, loads the master key, and creates n load
// accounts. With AccountSeed set, load keys are deterministically derived from
// a versioned domain-separated hash; otherwise they are crypto-random.
func NewPoolWithOptions(ctx context.Context, jsonrpc, masterKeyHex string, n int, expectedChainID uint64, opts PoolOptions) (*Pool, error) {
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
	seed, err := parseAccountSeed(opts.AccountSeed)
	if err != nil {
		return nil, err
	}
	if opts.AccountsFile != "" && len(seed) == 0 {
		return nil, fmt.Errorf("accounts file requires account seed")
	}
	for i := 0; i < n; i++ {
		var k *ecdsa.PrivateKey
		if len(seed) > 0 {
			k, err = deriveAccountKey(seed, uint64(i))
			if err != nil {
				return nil, fmt.Errorf("derive account %d: %w", i, err)
			}
		} else {
			k, err = crypto.GenerateKey()
			if err != nil {
				return nil, fmt.Errorf("generate account %d: %w", i, err)
			}
		}
		p.Accs = append(p.Accs, newAccount(k))
	}
	if opts.AccountsFile != "" {
		if err := WriteAccountsManifest(opts.AccountsFile, seed, p.Accs); err != nil {
			return nil, err
		}
	}
	// Seed the master's standard nonce from chain.
	if err := p.seedNonce(ctx, p.Master); err != nil {
		return nil, err
	}
	return p, nil
}

func parseAccountSeed(seedHex string) ([]byte, error) {
	seedHex = strings.TrimPrefix(strings.TrimSpace(seedHex), "0x")
	if seedHex == "" {
		return nil, nil
	}
	seed, err := hex.DecodeString(seedHex)
	if err != nil {
		return nil, fmt.Errorf("account seed must be hex: %w", err)
	}
	if len(seed) < minAccountSeedBytes {
		return nil, fmt.Errorf("account seed must be at least %d bytes", minAccountSeedBytes)
	}
	return seed, nil
}

func deriveAccountKey(seed []byte, index uint64) (*ecdsa.PrivateKey, error) {
	var idx [8]byte
	var retry [8]byte
	binary.BigEndian.PutUint64(idx[:], index)
	for r := uint64(0); r < math.MaxUint64; r++ {
		binary.BigEndian.PutUint64(retry[:], r)
		d := crypto.Keccak256([]byte(accountDeriveDomain), seed, idx[:], retry[:])
		k, err := crypto.ToECDSA(d)
		if err == nil {
			return k, nil
		}
	}
	return nil, fmt.Errorf("no valid secp256k1 scalar for account index %d", index)
}

// AccountSeedFingerprint returns a public, non-secret identifier for a seed.
func AccountSeedFingerprint(seedHex string) (string, error) {
	seed, err := parseAccountSeed(seedHex)
	if err != nil {
		return "", err
	}
	return accountSeedFingerprint(seed), nil
}

func accountSeedFingerprint(seed []byte) string {
	sum := crypto.Keccak256([]byte(accountFingerDomain), seed)
	return "0x" + hex.EncodeToString(sum[:16])
}

type accountsManifest struct {
	Version         int                     `json:"version"`
	SeedFingerprint string                  `json:"seedFingerprint"`
	Accounts        []accountsManifestEntry `json:"accounts"`
}

type accountsManifestEntry struct {
	Index   int    `json:"index"`
	Address string `json:"address"`
}

// WriteAccountsManifest atomically writes an address-only manifest. It never
// persists seeds or private scalars.
func WriteAccountsManifest(path string, seed []byte, accs []*Account) error {
	if len(seed) < minAccountSeedBytes {
		return fmt.Errorf("account seed must be at least %d bytes", minAccountSeedBytes)
	}
	m := accountsManifest{
		Version:         manifestVersion,
		SeedFingerprint: accountSeedFingerprint(seed),
		Accounts:        make([]accountsManifestEntry, 0, len(accs)),
	}
	for i, a := range accs {
		m.Accounts = append(m.Accounts, accountsManifestEntry{Index: i, Address: a.Addr.Hex()})
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal accounts manifest: %w", err)
	}
	raw = append(raw, '\n')
	if err := writeFileAtomic(path, raw, 0o644); err != nil {
		return fmt.Errorf("write accounts manifest %s: %w", path, err)
	}
	return nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (p *Pool) seedNonce(ctx context.Context, a *Account) error {
	n, err := PendingNonceAt(ctx, p.Client, a.Addr)
	if err != nil {
		return fmt.Errorf("pending nonce for %s: %w", a.Addr, err)
	}
	a.SetBase(0, n)
	return nil
}

// Fees returns (gasFeeCap, gasTipCap) from the current base fee. Callers on the
// hot path should fetch once and reuse, passing the values to the Sign* helpers.
func (p *Pool) Fees(ctx context.Context) (*big.Int, *big.Int, error) {
	head, err := retryQuery(ctx, func(ctx context.Context) (*types.Header, error) {
		return p.Client.HeaderByNumber(ctx, nil)
	})
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

// fundEndowments computes how much each account must RECEIVE from its parent:
// enough to finish with perAccount balance after paying descendant endowments
// and one gas charge for each non-zero child funding tx. Existing balances are
// credited exactly, so already-funded seeded pools produce zero transfers.
func fundEndowments(rounds [][]fundPair, balances []*big.Int, perAccount, gasPerTx *big.Int) []*big.Int {
	n := len(balances)
	required := make([]*big.Int, n)
	endow := make([]*big.Int, n)
	for i := range endow {
		required[i] = new(big.Int).Set(perAccount)
		endow[i] = new(big.Int)
	}
	for r := len(rounds) - 1; r >= 0; r-- {
		for _, fp := range rounds[r] {
			need := new(big.Int).Sub(required[fp.receiver], balances[fp.receiver])
			if need.Sign() > 0 {
				endow[fp.receiver] = need
			}
			if fp.sender >= 0 && endow[fp.receiver].Sign() > 0 {
				required[fp.sender].Add(required[fp.sender], endow[fp.receiver])
				required[fp.sender].Add(required[fp.sender], gasPerTx)
			}
		}
	}
	return endow
}

func rootFundingNeed(rounds [][]fundPair, endow []*big.Int, gasPerTx *big.Int) *big.Int {
	need := new(big.Int)
	for _, round := range rounds {
		for _, fp := range round {
			if fp.sender != -1 || endow[fp.receiver].Sign() == 0 {
				continue
			}
			need.Add(need, endow[fp.receiver])
			need.Add(need, gasPerTx)
		}
	}
	return need
}

func boundedWorkers(total, limit int) int {
	if total <= 0 {
		return 0
	}
	if limit <= 0 {
		return 1
	}
	if limit > total {
		return total
	}
	return limit
}

type accountChainState struct {
	balance *big.Int
	nonce   uint64
}

func (p *Pool) loadAccountStates(ctx context.Context, workers int) ([]accountChainState, error) {
	workers = boundedWorkers(len(p.Accs), workers)
	if workers == 0 {
		return nil, nil
	}
	states := make([]accountChainState, len(p.Accs))
	jobs := make(chan int)
	errCh := make(chan error, len(p.Accs))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				a := p.Accs[i]
				bal, err := BalanceAt(ctx, p.Client, a.Addr)
				if err != nil {
					errCh <- fmt.Errorf("read balance for %s: %w", a.Addr, err)
					continue
				}
				nonce, err := PendingNonceAt(ctx, p.Client, a.Addr)
				if err != nil {
					errCh <- fmt.Errorf("pending nonce for %s: %w", a.Addr, err)
					continue
				}
				a.SetBase(0, nonce)
				states[i] = accountChainState{balance: bal, nonce: nonce}
			}
		}()
	}
	for i := range p.Accs {
		select {
		case jobs <- i:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}
	p.statesMu.Lock()
	p.lastStates = states
	p.statesMu.Unlock()
	return states, nil
}

type sentFundingTx struct {
	hash common.Hash
	addr common.Address
}

func (p *Pool) waitFundingReceipts(ctx context.Context, txs []sentFundingTx, timeout time.Duration, workers int) error {
	workers = boundedWorkers(len(txs), workers)
	if workers == 0 {
		return nil
	}
	jobs := make(chan sentFundingTx)
	errCh := make(chan error, len(txs))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				if err := p.waitMined(ctx, s.hash, timeout); err != nil {
					errCh <- fmt.Errorf("wait funding tx to %s: %w", s.addr, err)
				}
			}
		}()
	}
	for _, tx := range txs {
		select {
		case jobs <- tx:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
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

	if err := p.seedNonce(ctx, p.Master); err != nil {
		return err
	}
	states, err := p.loadAccountStates(ctx, fundingWorkers)
	if err != nil {
		return err
	}
	balances := make([]*big.Int, len(states))
	for i, st := range states {
		balances[i] = st.balance
	}

	// Master-balance precheck: on a public testnet a faucet-limited master that
	// can't cover all root transfers plus gas would otherwise fund an arbitrary
	// prefix of accounts and then fail mid-stream (or time out waiting), leaving
	// the rest unfunded. Fail fast with a clear, actionable message instead.
	bal, err := BalanceAt(ctx, p.Client, p.Master.Addr)
	if err != nil {
		return fmt.Errorf("read master balance: %w", err)
	}
	gasPerTx := new(big.Int).Mul(big.NewInt(21000), feeCap)
	rounds := fundSchedule(len(p.Accs))
	endow := fundEndowments(rounds, balances, wei, gasPerTx)
	need := rootFundingNeed(rounds, endow, gasPerTx)
	if bal.Cmp(need) < 0 {
		return fmt.Errorf("master %s balance %s wei < required %s wei for root funding transfers; "+
			"fund the master or lower funding.accountsN / fundPerAccount", p.Master.Addr.Hex(), bal, need)
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
	// A wedged sender (errSlotWedged) costs its receiver a top-up but must not
	// abort the pass: an underfunded account simply retires from the load
	// rotation later. A HIGH skip count is different - that means something
	// systemic, so it still fails loudly rather than silently under-funding.
	maxWedged := len(p.Accs) / 100 // 1%
	if maxWedged < 8 {
		maxWedged = 8
	}
	// An account that is already present in the chain's auth store cannot be
	// CREATED by its funding tx, so that tx cannot hit the concurrent-creation
	// race described on sendFundingBatch - it is safe to broadcast in parallel.
	// This is what makes a re-fund fast: every account of a previously funded
	// pool already exists, so the whole pass parallelises.
	exists := make([]bool, len(p.Accs))
	for i, st := range states {
		exists[i] = st.nonce > 0 || (st.balance != nil && st.balance.Sign() > 0)
	}

	funded, wedged := 0, 0
	for ri, round := range rounds {
		concurrent, serial := partitionRound(round, endow, exists)
		if len(concurrent) == 0 && len(serial) == 0 {
			continue // no-op round (common: a grown seeded pool skips every early round)
		}

		txs, w, err := p.sendFundingBatch(ctx, concurrent, endow, feeCap, tip, fundingBroadcastWorkers)
		wedged += w
		if err != nil {
			return err
		}
		stxs, sw, err := p.sendFundingBatch(ctx, serial, endow, feeCap, tip, 1)
		wedged += sw
		if err != nil {
			return err
		}
		txs = append(txs, stxs...)
		if wedged > maxWedged {
			return fmt.Errorf("%d funding senders unusable (limit %d) - wedged nonce slots in the "+
				"chain's app mempool, or their own top-ups were skipped; restart the target node "+
				"to clear stale mempool entries", wedged, maxWedged)
		}
		if err := p.waitFundingReceipts(ctx, txs, 60*time.Second, fundingWorkers); err != nil {
			return err
		}
		funded += len(txs)
		log.Printf("[fund] round %d/%d mined: %d top-up txs (%d parallel, %d serial-new-account) (%d/%d total)",
			ri+1, len(rounds), len(txs), len(concurrent), len(serial), funded, len(p.Accs))
	}
	if wedged > 0 {
		log.Printf("[fund] %d/%d accounts left without a top-up (wedged sender nonce slots); "+
			"they will retire from the load rotation if underfunded", wedged, len(p.Accs))
	}

	// Re-seed every account's nonce from chain: tree senders consumed nonces.
	if _, err := p.loadAccountStates(ctx, fundingWorkers); err != nil {
		return err
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
// total amount recovered (in wei) is returned. A fixed-size worker pool bounds
// balance/receipt RPC concurrency even when the reusable account pool is large.
// MinBalance returns the balance of the POOREST account in the pool, which is
// the account that decides whether a run survives: the ready-queue rotation is
// only roughly fair, so the weakest account is bankrupted first and every
// retirement concentrates more txs onto the survivors. A mean would hide exactly
// the tail that matters - a pool split between richly-funded reused accounts and
// thinly-funded new ones (the common case for a grown seeded pool) has a healthy
// mean and a fatal minimum.
func (p *Pool) MinBalance(ctx context.Context) (*big.Int, error) {
	// Fund ends by re-reading every account, so in the normal start-up order the
	// balances are already in hand and re-reading them would add a second
	// 2-RPC-per-account sweep of the pool for no new information.
	p.statesMu.Lock()
	states := p.lastStates
	p.statesMu.Unlock()
	if len(states) != len(p.Accs) {
		var err error
		if states, err = p.loadAccountStates(ctx, fundingWorkers); err != nil {
			return nil, err
		}
	}
	var min *big.Int
	for _, st := range states {
		if st.balance == nil {
			continue
		}
		if min == nil || st.balance.Cmp(min) < 0 {
			min = st.balance
		}
	}
	if min == nil {
		return nil, fmt.Errorf("no account balances read")
	}
	return min, nil
}

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
	jobs := make(chan int)
	var wg sync.WaitGroup
	workers := boundedWorkers(len(p.Accs), sweepWorkers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				a := p.Accs[i]
				skip := func(format string, args ...any) {
					log.Printf("[sweep] %d/%d: "+format, append([]any{i + 1, len(p.Accs)}, args...)...)
					mu.Lock()
					skipped++
					mu.Unlock()
				}
				bal, err := BalanceAt(ctx, p.Client, a.Addr)
				if err != nil {
					skip("balance read failed for %s: %v (skipped)", a.Addr, err)
					continue
				}
				amount := sweepAmount(bal, gasReserve)
				if amount == nil {
					mu.Lock()
					skipped++ // dust: not enough to cover the sweep's own gas
					mu.Unlock()
					continue
				}
				nonce, err := PendingNonceAt(ctx, p.Client, a.Addr)
				if err != nil {
					skip("nonce read failed for %s: %v (skipped)", a.Addr, err)
					continue
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
					continue
				}
				sendMu.Lock()
				err = p.sendWithRetry(ctx, signed)
				sendMu.Unlock()
				if err != nil {
					skip("send failed for %s: %v (skipped)", a.Addr, err)
					continue
				}
				if err := p.waitMined(ctx, signed.Hash(), 60*time.Second); err != nil {
					skip("not mined for %s: %v (funds may still return)", a.Addr, err)
					continue
				}
				mu.Lock()
				recovered.Add(recovered, amount)
				swept++
				mu.Unlock()
			}
		}()
	}
	for i := range p.Accs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	log.Printf("[sweep] recovered %s wei to master from %d/%d accounts (%d skipped)",
		recovered, swept, len(p.Accs), skipped)
	return recovered, nil
}

// partitionRound splits one funding round into the pairs that may be broadcast
// CONCURRENTLY and the pairs that must be broadcast SERIALLY, dropping any pair
// with nothing to send.
//
// The rule is a safety invariant, not a heuristic: a funding tx to an account
// that is not yet in the chain's auth store CREATES it, and two such creations
// executing concurrently panic the node in the fee path (x/auth "index
// uniqueness constrain violation"). A receiver that already exists cannot be
// created again, so its transfer is free of that hazard. Getting this backwards
// does not slow a run down, it panics CheckTx on the node under test - so keep
// the default on the safe side: anything not known to exist goes serial.
func partitionRound(round []fundPair, endow []*big.Int, exists []bool) (concurrent, serial []fundPair) {
	for _, fp := range round {
		if fp.receiver >= len(endow) || endow[fp.receiver] == nil || endow[fp.receiver].Sign() == 0 {
			continue // already holds enough; nothing to send
		}
		if fp.receiver < len(exists) && exists[fp.receiver] {
			concurrent = append(concurrent, fp)
		} else {
			serial = append(serial, fp)
		}
	}
	return concurrent, serial
}

// sendFundingBatch broadcasts one round's transfers at the given concurrency and
// returns the sent txs plus the number skipped for wedged/broke senders.
//
// Concurrency is safe here for a specific, narrow reason. Within a round every
// pair has a DISTINCT sender, so the chain's 1-in-flight-per-sender rule is
// never violated and no two goroutines touch the same Account. What forced this
// to be serial was a different hazard: two CheckTx executions racing to CREATE a
// not-yet-existing account panic the node in the fee path (x/auth "index
// uniqueness constrain violation" - see Fund). That race needs an account
// CREATION, so callers pass workers>1 only for receivers already present in the
// auth store, and workers=1 for the rest.
//
// Why it matters: broadcasts dominate a funding pass. Topping a 36000-account
// pool up from 15000 funded is 21000 transfers, which at ~10ms each is ~3.5
// minutes serial and ~1.6s at 128 concurrency.
//
// Unlike the serial version this does not abort mid-round on the wedged limit;
// the caller checks the running total once the round completes. A round is
// bounded work, and the limit is a health signal rather than a safety stop.
func (p *Pool) sendFundingBatch(ctx context.Context, pairs []fundPair, endow []*big.Int, feeCap, tip *big.Int, workers int) ([]sentFundingTx, int, error) {
	if len(pairs) == 0 {
		return nil, 0, nil
	}
	workers = boundedWorkers(len(pairs), workers)

	var (
		mu      sync.Mutex
		txs     []sentFundingTx
		wedged  int
		hardErr error
	)
	jobs := make(chan fundPair)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fp := range jobs {
				mu.Lock()
				stop := hardErr != nil
				mu.Unlock()
				if stop || ctx.Err() != nil {
					continue // drain: a peer already failed hard
				}
				sender := p.Master
				if fp.sender >= 0 {
					sender = p.Accs[fp.sender]
				}
				recv := p.Accs[fp.receiver]
				hash, err := p.sendFundingTx(ctx, sender, recv.Addr, endow[fp.receiver], feeCap, tip)
				mu.Lock()
				switch {
				case errors.Is(err, errSlotWedged) || errors.Is(err, errSenderBroke):
					wedged++
					log.Printf("[fund] WARNING: skipping top-up of %s: %v (%d skipped)", recv.Addr, err, wedged)
				case err != nil:
					if hardErr == nil {
						hardErr = fmt.Errorf("send funding tx to %s: %w", recv.Addr, err)
					}
				default:
					txs = append(txs, sentFundingTx{hash: hash, addr: recv.Addr})
				}
				mu.Unlock()
			}
		}()
	}
	for _, fp := range pairs {
		jobs <- fp
	}
	close(jobs)
	wg.Wait()
	return txs, wedged, hardErr
}

// sendFundingTx broadcasts ONE funding transfer and returns its hash, resyncing
// the sender's nonce whenever chain state has moved past our local view.
//
// sendWithRetry cannot do this job: it replays the same signed bytes, and a tx
// whose nonce is already consumed is rejected identically on every attempt.
// Nonces drift out from under us for several ordinary reasons -
//
//   - another loadtester process shares this master key (the classic one: three
//     targets pointing at three RPC nodes but all naming the same funding key,
//     which produced "got 58, expected 59: txnonce is lower than account nonce")
//   - an earlier funding pass aborted after broadcasting but before we recorded it
//   - the nonce was seeded from an endpoint whose committed state lagged
//
// so the recovery is always the same: re-read the COMMITTED nonce, re-sign at
// that nonce, and send again. Committed (not pending) is the right basis - this
// chain admits only nonce == committed nonce, with no future-nonce queue.
func (p *Pool) sendFundingTx(ctx context.Context, sender *Account, to common.Address, value, feeCap, tip *big.Int) (common.Hash, error) {
	const attempts = 8
	var lastErr error
	for i := 1; i <= attempts; i++ {
		nonce := sender.Peek(0)
		signed, err := types.SignTx(types.NewTx(&types.DynamicFeeTx{
			ChainID:   p.ChainID,
			Nonce:     nonce,
			GasTipCap: tip,
			GasFeeCap: feeCap,
			Gas:       21000,
			To:        &to,
			Value:     value,
		}), p.Signer, sender.Key)
		if err != nil {
			return common.Hash{}, fmt.Errorf("sign funding tx: %w", err)
		}
		err = p.Client.SendTransaction(ctx, signed)
		if err == nil || isAlreadyKnownErr(err) {
			sender.SetBase(0, nonce+1) // only a landed broadcast consumes the nonce
			return signed.Hash(), nil
		}
		lastErr = err
		switch {
		case isNonceMismatchErr(err):
			// Adopt chain's view and re-sign. Do NOT fall back to the local
			// nonce on a failed read - that would resend into the same wall.
			n, qerr := CommittedNonceAt(ctx, p.Client, sender.Addr)
			if qerr != nil {
				return common.Hash{}, fmt.Errorf("resync nonce for %s after %v: %w", sender.Addr, err, qerr)
			}
			log.Printf("[fund] %s nonce resync %d -> %d (%v)", sender.Addr, nonce, n, err)
			sender.SetBase(0, n)
		case isSlotOccupiedErr(err):
			// A stale tx (typically a previous run's load tx) holds this slot.
			// If chain state has since moved past it, re-sign at the new nonce;
			// otherwise the slot is unrecoverable here (see errSlotWedged) and
			// the caller must skip this transfer.
			n, qerr := CommittedNonceAt(ctx, p.Client, sender.Addr)
			if qerr != nil {
				return common.Hash{}, fmt.Errorf("resync nonce for %s after %v: %w", sender.Addr, err, qerr)
			}
			if n > nonce {
				sender.SetBase(0, n)
				break
			}
			return common.Hash{}, fmt.Errorf("%s nonce %d: %w", sender.Addr, nonce, errSlotWedged)
		case isBrokeErr(err):
			return common.Hash{}, fmt.Errorf("%s: %w", sender.Addr, errSenderBroke)
		case isTransientQueryErr(err):
			// Node-side race or transport flake: the same nonce is still ours.
		default:
			return common.Hash{}, err // definitive rejection
		}
		if i == attempts {
			break
		}
		select {
		case <-ctx.Done():
			return common.Hash{}, ctx.Err()
		case <-time.After(time.Duration(i) * 400 * time.Millisecond):
		}
	}
	return common.Hash{}, lastErr
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
