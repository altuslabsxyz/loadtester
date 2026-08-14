// Package config loads the load-tester target environment description.
//
// A Target fully describes one environment (local init.sh chain or a remote
// testnet). Switching environments is a config change, never a code change.
package config

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"gopkg.in/yaml.v3"
)

// Role classifies a node for reporting / app-hash comparison.
type Role string

const (
	RoleValidator  Role = "validator"
	RoleFullnode   Role = "fullnode"
	RoleGuaranteed Role = "guaranteed"
	RoleEnterprise Role = "enterprise"
	RoleVIP        Role = "vip" // legacy target compatibility
)

// GovMode selects how Tx-Type lanes get registered.
//
//   - fast-pass:    submit a proposal and vote YES from all votingNodes; assumes
//     a short voting period (local single/few-validator chain).
//   - real-vote:    submit + vote, then wait out the real voting period.
//   - preconfigured: skip registration; query current chain params and feed the
//     classifier. Use when an operator already configured the lanes.
type GovMode string

const (
	GovFastPass      GovMode = "fast-pass"
	GovRealVote      GovMode = "real-vote"
	GovPreconfigured GovMode = "preconfigured"
)

// Node is a single reachable node and its endpoints.
type Node struct {
	Name     string `yaml:"name"`
	Role     Role   `yaml:"role"`
	JSONRPC  string `yaml:"jsonrpc"`
	CometRPC string `yaml:"cometRPC"`
	// GRPC is the cosmos gRPC endpoint (e.g. 127.0.0.1:9090). Optional; used to
	// query x/stable params (lane verification / preconfigured mode).
	GRPC string `yaml:"grpc"`
}

// Funding describes the master key and the fan-out account pool.
type Funding struct {
	MasterKey      string `yaml:"masterKey"`      // hex private key, with or without 0x
	AccountsN      int    `yaml:"accountsN"`      // number of load accounts to generate
	FundPerAccount string `yaml:"fundPerAccount"` // decimal whole gas tokens, e.g. "0.01" or "1" (fractional supported; ×1e18 to wei)
	// AccountSeed, when set, derives load-account private keys from this hex
	// seed using versioned domain-separated hashing. The same seed always yields
	// the same N addresses, so a pool can be REUSED
	// across runs: fund it once, and later runs top up only what is underfunded
	// (no new accounts created). Empty = ephemeral random accounts (legacy: keys
	// exist only in memory for the run and are swept back at the end).
	AccountSeed string `yaml:"accountSeed"`
	// AccountsFile, when non-empty, is the path the derived address list is
	// written to after the pool is built (addresses + index + seed fingerprint,
	// for reference / external funding / committing to the repo). The private
	// keys are NEVER written - they stay derived from the seed at runtime.
	AccountsFile string `yaml:"accountsFile"`
	// SweepBack controls whether, at the end of a one-shot run, each load
	// account's leftover native balance is returned to the master (minus one
	// tx of gas). Default (nil/omitted): ON for ephemeral random accounts (whose
	// keys are lost at exit), OFF when accountSeed is set (funds stay in the
	// reusable pool for the next run). Set explicitly to override either default.
	// No effect in continuous mode (Ctrl+C interrupts before a sweep can run).
	SweepBack *bool `yaml:"sweepBack"`
}

// ShouldSweep reports whether end-of-run fund recovery is enabled. Default: on
// for ephemeral (random) accounts so funds are not stranded in lost keys; off
// for a seeded, reusable pool so the funds persist for the next run.
func (f Funding) ShouldSweep() bool {
	if f.SweepBack != nil {
		return *f.SweepBack
	}
	return f.AccountSeed == ""
}

// Governance describes lane-registration behavior.
type Governance struct {
	Mode GovMode `yaml:"mode"`
	// ProposerKey funds the deposit and submits the proposal.
	ProposerKey string `yaml:"proposerKey"`
	// VoterKeys vote YES. For fast-pass/real-vote these must cover quorum.
	VoterKeys []string `yaml:"voterKeys"`
	// Deposit is the proposal deposit, e.g. "50000000000000000000000astable".
	Deposit string `yaml:"deposit"`
	// VotingNode is the node index used to submit/vote (default 0).
	VotingNode int `yaml:"votingNode"`
}

// LaneLoad is the per-lane saturation target.
type LaneLoad struct {
	// TargetInflight is the number of in-flight (submitted, unconfirmed) txs to
	// sustain for this workload. Higher = more oversubscription.
	TargetInflight int `yaml:"targetInflight"`
	// Enabled allows disabling a workload (e.g. destructive ones on testnet).
	Enabled *bool `yaml:"enabled"`
}

// Workload describes the load phase.
type Workload struct {
	DurationSec int                 `yaml:"durationSec"`
	Lanes       map[string]LaneLoad `yaml:"lanes"`
	// Workers bounds load-generation goroutines. Default 256.
	Workers int `yaml:"workers"`
	// TargetTPS is the optional aggregate send-rate cap. 0 means uncapped.
	TargetTPS int `yaml:"targetTPS"`
	// TipRampWeiPerSec adds a steadily rising premium to each tx's tip so newer
	// txs outrank older ones in the node's priority-ordered mempool. 0 (default)
	// keeps a flat tip. See workload.Driver.SetTipRamp for the full rationale:
	// the tx-provider fills its reap window from the highest-priority txs, and a
	// flat tip makes that window the OLDEST txs - the ones already included but
	// not yet pruned, which the proposer then discards as stale.
	//
	// The premium grows without bound over a run, so keep runs bounded (or the
	// value small): at 1e8 (0.1 gwei/s) a 150s run ends ~15 gwei above base.
	TipRampWeiPerSec int64 `yaml:"tipRampWeiPerSec"`
	// RecipientPoolSize controls shared-recipient contention for transfer workloads.
	//   0 (default): one deterministic recipient per sender, for parallel-capacity tests.
	//   1:           one shared hot recipient, reproducing the legacy contention test.
	//   N > 1:       deterministically shard senders across N shared recipients.
	RecipientPoolSize int `yaml:"recipientPoolSize"`
	// AllowDestructive gates SELFDESTRUCT / heavy determinism scenarios.
	// Defaults false (testnet-safe); must be explicitly enabled.
	AllowDestructive bool `yaml:"allowDestructive"`
	// PerAccountInflight caps how many ordered txs ONE account may have in the
	// mempool at once, per nonce-key. 0 uses workload.DefaultPerAccountInflight
	// (1).
	//
	// Raising it lifts the in-flight ceiling off accountsN - at depth 1 an
	// account is out of rotation until its tx is confirmed, so the pool size IS
	// the in-flight cap. But measured on stable_988-1 it LOWERED throughput
	// (4,019 -> 2,486 tx/s from depth 1 to 2), because a fixed in-flight budget
	// at depth D involves only accountsN/D distinct senders and one sender's txs
	// are nonce-ordered, so they cannot execute in parallel. Prefer raising
	// accountsN. See workload.DefaultPerAccountInflight for the full numbers.
	//
	// The product accountsN*perAccountInflight is the worst-case backlog and
	// must stay under the node's mempool size (config.toml mempool.size).
	PerAccountInflight int `yaml:"perAccountInflight"`
	// MaxInflight caps the TOTAL txs the driver keeps in the endpoint's mempool.
	// 0 means accountsN*perAccountInflight (i.e. no extra cap).
	//
	// This is the knob for producing a STEADY block size instead of a volatile
	// one. Left uncapped, the driver simply fills the node's mempool - on
	// stable_988-1 that is 30,000 txs - and the proposer then takes as much of
	// it as it can in one block. Those oversized blocks take longer to execute
	// than a block interval, the tx-provider full node drifts past its
	// max_height_lag, it refuses to serve while it catches up, and the chain
	// produces a run of empty blocks before dumping the whole mempool again.
	// Measured on this devnet: block sizes alternating between 1 and 30,000.
	//
	// Setting this to roughly (target tx/s x block time x 1.5) keeps the backlog
	// at about one and a half blocks - deep enough that a proposer never finds
	// the mempool short, shallow enough that no block is oversized. For 10,000
	// tx/s at 1.0s blocks, 15000.
	MaxInflight int `yaml:"maxInflight"`
	// TargetMempoolDepth makes the driver hold the send endpoint's mempool at
	// this many transactions, measured every 100ms via CometBFT
	// num_unconfirmed_txs. 0 disables the controller and falls back to
	// MaxInflight. Requires a reachable cometRPC on the send node.
	//
	// This is the setting that decides block size, and it is the one to tune for
	// "N transactions in every block". The proposer takes what the provider has,
	// so a mempool held at N yields blocks of about N. Prefer it over
	// MaxInflight: MaxInflight counts txs the driver has not yet CONFIRMED,
	// which on this chain includes txs the validators already mined, so it
	// throttles the driver for seconds after every block and leaves the mempool
	// empty exactly when the next proposer needs it.
	//
	// Keep it under the EIP-1559 gas target (25,000 txs at max_gas 1.05B) so
	// blocks never cross 50% and push the base fee up.
	TargetMempoolDepth int `yaml:"targetMempoolDepth"`
}

// Observe configures the collectors.
type Observe struct {
	PollIntervalMs int `yaml:"pollIntervalMs"`
	DrainWindowSec int `yaml:"drainWindowSec"`
	// StuckAfterBlocks flags a tx resident longer than this many blocks while
	// neither included nor invalidated.
	StuckAfterBlocks int `yaml:"stuckAfterBlocks"`
	// ReportIntervalSec is the snapshot cadence in continuous mode (writes a
	// fresh report every N seconds). Default 30.
	ReportIntervalSec int `yaml:"reportIntervalSec"`
}

// Continuous reports whether the load should run until interrupted (SIGINT)
// rather than for a fixed duration. Enabled by workload.durationSec <= 0.
func (w Workload) Continuous() bool { return w.DurationSec <= 0 }

// LaneDef defines one lane to register, in config (instead of hardcoded). VIP
// lanes set vip:true (matched by nonce-key bit, no tx matchers). Tx-type lanes
// use the matcher fields. Address fields accept "0x.." or "@name" deployment
// refs; methods accept 4-byte hex selectors like "0xa9059cbb".
type LaneDef struct {
	ID         int32    `yaml:"id"`
	Name       string   `yaml:"name"`
	Weight     uint32   `yaml:"weight"`
	Enterprise bool     `yaml:"enterprise"`
	VIP        bool     `yaml:"vip"` // legacy target compatibility
	ToAddrs    []string `yaml:"toAddrs"`
	Methods    []string `yaml:"methods"`
	TxTypes    []string `yaml:"txTypes"` // LEGACY|ACCESS_LIST|DYNAMIC_FEE|SET_CODE|TWO_D_NONCE
	NonceKeys  []uint64 `yaml:"nonceKeys"`
	Senders    []string `yaml:"senders"`
	NoOverflow bool     `yaml:"noOverflow"`
}

// Blockspace is the config-driven lane plan. When Lanes is empty the harness
// falls back to the built-in uniswap/ERC20 preset.
type Blockspace struct {
	MaxBlockspaceGasWeight uint32    `yaml:"maxBlockspaceGasWeight"`
	Lanes                  []LaneDef `yaml:"lanes"`
}

// Target is the whole environment description.
type Target struct {
	Name          string     `yaml:"name"`
	ChainID       uint64     `yaml:"chainId"`       // EVM eip155 chain id (build-time injected; local=999)
	CosmosChainID string     `yaml:"cosmosChainId"` // e.g. stable_988-1
	Nodes         []Node     `yaml:"nodes"`
	Funding       Funding    `yaml:"funding"`
	Governance    Governance `yaml:"governance"`
	Blockspace    Blockspace `yaml:"blockspace"`
	Workload      Workload   `yaml:"workload"`
	Observe       Observe    `yaml:"observe"`
	// LogPaths are node log files to scrape for ground-truth signals
	// (lane-quota enforcement, app-hash mismatch/halt). Local-only; empty on
	// testnet where node logs are not reachable.
	LogPaths []string `yaml:"logPaths"`
}

// Load reads and validates a target.yaml.
func Load(path string) (*Target, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read target %s: %w", path, err)
	}
	var t Target
	if err := yaml.Unmarshal(raw, &t); err != nil {
		return nil, fmt.Errorf("parse target %s: %w", path, err)
	}
	t.applyDefaults()
	if err := t.validate(); err != nil {
		return nil, fmt.Errorf("invalid target %s: %w", path, err)
	}
	return &t, nil
}

func (t *Target) applyDefaults() {
	for i := range t.Blockspace.Lanes {
		isEnterprise := t.Blockspace.Lanes[i].Enterprise || t.Blockspace.Lanes[i].VIP
		t.Blockspace.Lanes[i].Enterprise = isEnterprise
		t.Blockspace.Lanes[i].VIP = isEnterprise
	}
	if t.Observe.PollIntervalMs == 0 {
		t.Observe.PollIntervalMs = 200
	}
	if t.Observe.DrainWindowSec == 0 {
		t.Observe.DrainWindowSec = 60
	}
	if t.Observe.StuckAfterBlocks == 0 {
		t.Observe.StuckAfterBlocks = 20
	}
	if t.Governance.Mode == "" {
		t.Governance.Mode = GovPreconfigured
	}
	if t.Workload.Workers == 0 {
		t.Workload.Workers = 256
	}
	// NOTE: durationSec is intentionally NOT defaulted. durationSec <= 0 means
	// CONTINUOUS (run until interrupted); a positive value means one-shot.
}

func (t *Target) validate() error {
	if t.ChainID == 0 {
		return fmt.Errorf("chainId must be set (EVM eip155 id; local init build = 999)")
	}
	if len(t.Nodes) == 0 {
		return fmt.Errorf("at least one node is required")
	}
	if t.Funding.AccountsN <= 0 {
		return fmt.Errorf("funding.accountsN must be > 0")
	}
	if strings.TrimSpace(t.Funding.MasterKey) == "" {
		return fmt.Errorf("funding.masterKey is required")
	}
	if _, err := crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(t.Funding.MasterKey), "0x")); err != nil {
		return fmt.Errorf("funding.masterKey is not parseable: %w", err)
	}
	fundWei, err := parsePositiveWholeTokensToWei(t.Funding.FundPerAccount)
	if err != nil {
		return err
	}
	if fundWei.Sign() <= 0 {
		return fmt.Errorf("funding.fundPerAccount must be > 0")
	}
	if strings.TrimSpace(t.Funding.AccountSeed) != "" {
		seed, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(t.Funding.AccountSeed), "0x"))
		if err != nil {
			return fmt.Errorf("funding.accountSeed must be hex: %w", err)
		}
		if len(seed) < 32 {
			return fmt.Errorf("funding.accountSeed must be at least 32 bytes")
		}
	} else if strings.TrimSpace(t.Funding.AccountsFile) != "" {
		return fmt.Errorf("funding.accountsFile requires funding.accountSeed")
	}
	if t.Workload.RecipientPoolSize < 0 {
		return fmt.Errorf("workload.recipientPoolSize must be >= 0")
	}
	if t.Workload.Workers <= 0 || t.Workload.Workers > 4096 {
		return fmt.Errorf("workload.workers must be between 1 and 4096")
	}
	if t.Workload.TargetTPS < 0 || t.Workload.TargetTPS > 1_000_000_000 {
		return fmt.Errorf("workload.targetTPS must be between 0 and 1000000000")
	}
	if t.Workload.TipRampWeiPerSec < 0 {
		return fmt.Errorf("workload.tipRampWeiPerSec must be >= 0")
	}
	if t.Workload.PerAccountInflight < 0 || t.Workload.PerAccountInflight > 64 {
		return fmt.Errorf("workload.perAccountInflight must be between 0 (default) and 64")
	}
	if t.Workload.MaxInflight < 0 {
		return fmt.Errorf("workload.maxInflight must be >= 0 (0 = accountsN*perAccountInflight)")
	}
	if t.Workload.TargetMempoolDepth < 0 {
		return fmt.Errorf("workload.targetMempoolDepth must be >= 0 (0 = disabled)")
	}
	laneNames := make([]string, 0, len(t.Workload.Lanes))
	for name := range t.Workload.Lanes {
		laneNames = append(laneNames, name)
	}
	sort.Strings(laneNames)
	for _, name := range laneNames {
		lane := t.Workload.Lanes[name]
		if lane.TargetInflight < 0 {
			return fmt.Errorf("workload.lanes.%s.targetInflight must be >= 0", name)
		}
	}
	for i, n := range t.Nodes {
		if n.JSONRPC == "" {
			return fmt.Errorf("node[%d] %q missing jsonrpc", i, n.Name)
		}
	}
	switch t.Governance.Mode {
	case GovFastPass, GovRealVote:
		if t.Governance.ProposerKey == "" {
			return fmt.Errorf("governance.mode %s requires proposerKey", t.Governance.Mode)
		}
		if len(t.Governance.VoterKeys) == 0 {
			return fmt.Errorf("governance.mode %s requires voterKeys", t.Governance.Mode)
		}
	case GovPreconfigured:
		// nothing required
	default:
		return fmt.Errorf("unknown governance.mode %q", t.Governance.Mode)
	}
	return nil
}

func parsePositiveWholeTokensToWei(s string) (*big.Int, error) {
	s = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(s), "+"))
	if s == "" {
		return nil, fmt.Errorf("funding.fundPerAccount is required")
	}
	if strings.HasPrefix(s, "-") {
		return nil, fmt.Errorf("funding.fundPerAccount must not be negative")
	}
	intPart, fracPart, hasFrac := strings.Cut(s, ".")
	if intPart == "" {
		intPart = "0"
	}
	if intPart == "" && !hasFrac {
		return nil, fmt.Errorf("funding.fundPerAccount is invalid")
	}
	if len(fracPart) > 18 {
		fracPart = fracPart[:18]
	}
	fracPart += strings.Repeat("0", 18-len(fracPart))
	wei, ok := new(big.Int).SetString(intPart+fracPart, 10)
	if !ok {
		return nil, fmt.Errorf("funding.fundPerAccount is invalid")
	}
	return wei, nil
}

// PrimaryJSONRPC returns the JSON-RPC endpoint used for sending load txs.
// It prefers the first fullnode, then any node.
func (t *Target) PrimaryJSONRPC() string {
	for _, n := range t.Nodes {
		if n.Role == RoleFullnode {
			return n.JSONRPC
		}
	}
	return t.Nodes[0].JSONRPC
}

// VIPJSONRPC returns the dedicated JSON-RPC endpoint for guaranteed Enterprise
// transactions. The legacy name is retained for target compatibility.
func (t *Target) VIPJSONRPC() string {
	for _, n := range t.Nodes {
		if (n.Role == RoleGuaranteed || n.Role == RoleEnterprise || n.Role == RoleVIP) && strings.TrimSpace(n.JSONRPC) != "" {
			return n.JSONRPC
		}
	}
	return ""
}

// BlockFeedJSONRPC returns a JSON-RPC endpoint to read COMMITTED BLOCK CONTENTS
// from, which is deliberately allowed to differ from the send endpoint.
//
// The send engine needs two different facts per block, with very different
// costs. "Which of my txs are in block H" is consensus-agreed, so any node that
// has committed H gives the identical answer. "Has the node I send to committed
// past H" must come from the send endpoint itself, and is one cheap
// eth_blockNumber.
//
// Reading contents from the send endpoint is expensive on exactly the wrong
// machine. On stable_988-1 the send endpoint is the tx-provider full node, and
// eth_getBlockByNumber there does CometBlockByNumber + BlockResults + convert
// (stable-evm rpc/backend/blocks.go GetBlockByNumber) - for a 40,000-tx block
// that is every tx result and its events, once per block, on the one node whose
// falling behind already costs the chain whole empty blocks.
//
// So prefer any OTHER configured node for contents. Returns "" when the target
// has only the send endpoint, in which case the engine reads both from it.
func (t *Target) BlockFeedJSONRPC() string {
	primary := t.PrimaryJSONRPC()
	// Prefer a validator: it neither serves the tx provider nor runs the EVM
	// indexer, so the extra reads land on the least loaded node.
	for _, n := range t.Nodes {
		if n.Role == RoleValidator && strings.TrimSpace(n.JSONRPC) != "" && n.JSONRPC != primary {
			return n.JSONRPC
		}
	}
	for _, n := range t.Nodes {
		if strings.TrimSpace(n.JSONRPC) != "" && n.JSONRPC != primary {
			return n.JSONRPC
		}
	}
	return ""
}

// PrimaryCometRPC returns the CometBFT RPC of the node load is SENT to, which
// is the only node whose mempool depth is meaningful for flow control.
func (t *Target) PrimaryCometRPC() string {
	primary := t.PrimaryJSONRPC()
	for _, n := range t.Nodes {
		if n.JSONRPC == primary && strings.TrimSpace(n.CometRPC) != "" {
			return n.CometRPC
		}
	}
	return ""
}

// FreshSupplyCometRPC returns a CometBFT RPC on a node OTHER than the send
// endpoint, for counting committed transactions.
//
// It must not be the send endpoint. That node is the tx provider, it executes
// blocks a beat behind the validators, and its committed count carries exactly
// the lag the fresh-supply controller exists to cancel. A validator is caught up
// by construction - it cannot commit a block it has not executed.
func (t *Target) FreshSupplyCometRPC() string {
	primary := t.PrimaryCometRPC()
	for _, n := range t.Nodes {
		if n.Role == RoleValidator && strings.TrimSpace(n.CometRPC) != "" && n.CometRPC != primary {
			return n.CometRPC
		}
	}
	for _, n := range t.Nodes {
		if strings.TrimSpace(n.CometRPC) != "" && n.CometRPC != primary {
			return n.CometRPC
		}
	}
	return ""
}

// CometRPCs returns all configured CometBFT RPC endpoints (for app-hash compare).
func (t *Target) CometRPCs() []Node {
	out := make([]Node, 0, len(t.Nodes))
	for _, n := range t.Nodes {
		if strings.TrimSpace(n.CometRPC) != "" {
			out = append(out, n)
		}
	}
	return out
}
