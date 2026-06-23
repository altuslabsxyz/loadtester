// Package config loads the load-tester target environment description.
//
// A Target fully describes one environment (local init.sh chain or a remote
// testnet). Switching environments is a config change, never a code change.
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Role classifies a node for reporting / app-hash comparison.
type Role string

const (
	RoleValidator Role = "validator"
	RoleFullnode  Role = "fullnode"
	RoleVIP       Role = "vip"
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
	FundPerAccount string `yaml:"fundPerAccount"` // human amount in whole gas tokens (e.g. "1")
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
	// AllowDestructive gates SELFDESTRUCT / heavy determinism scenarios.
	// Defaults false (testnet-safe); must be explicitly enabled.
	AllowDestructive bool `yaml:"allowDestructive"`
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
	VIP        bool     `yaml:"vip"`
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
	if t.Funding.MasterKey == "" && t.Governance.Mode != GovPreconfigured {
		// master key needed to fund load accounts in any active load run
	}
	return nil
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

// VIPJSONRPC returns the JSON-RPC endpoint VIP txs must be sent to: the first
// node with role "vip". VIP (2D-nonce) txs are only accepted by that node, so
// when no vip-role node is configured this returns "" and the harness skips the
// VIP workload entirely.
func (t *Target) VIPJSONRPC() string {
	for _, n := range t.Nodes {
		if n.Role == RoleVIP && strings.TrimSpace(n.JSONRPC) != "" {
			return n.JSONRPC
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
