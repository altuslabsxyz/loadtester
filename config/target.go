// Package config loads the load-tester target environment description.
//
// A Target describes one EVM + CometBFT environment. Switching environments is
// a config change, never a code change.
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

// Node is a single reachable node and its endpoints.
type Node struct {
	Name     string `yaml:"name"`
	Role     Role   `yaml:"role"`
	JSONRPC  string `yaml:"jsonrpc"`
	CometRPC string `yaml:"cometRPC"`
}

// Funding describes the master key and the fan-out account pool.
type Funding struct {
	MasterKey      string `yaml:"masterKey"`      // hex private key, with or without 0x
	AccountsN      int    `yaml:"accountsN"`      // number of load accounts to generate
	FundPerAccount string `yaml:"fundPerAccount"` // whole gas tokens per account (e.g. "1")
}

// LaneLoad is the per-workload load setting.
type LaneLoad struct {
	// TargetInflight is the in-flight tx level to sustain for this workload.
	TargetInflight int `yaml:"targetInflight"`
	// Enabled allows disabling a workload without removing it.
	Enabled *bool `yaml:"enabled"`
}

// Workload describes the load phase.
type Workload struct {
	DurationSec      int                 `yaml:"durationSec"`
	Lanes            map[string]LaneLoad `yaml:"lanes"`
	AllowDestructive bool                `yaml:"allowDestructive"`
}

// Continuous reports whether the load runs until interrupted (durationSec <= 0).
func (w Workload) Continuous() bool { return w.DurationSec <= 0 }

// Observe configures the collectors.
type Observe struct {
	PollIntervalMs    int `yaml:"pollIntervalMs"`
	DrainWindowSec    int `yaml:"drainWindowSec"`
	ReportIntervalSec int `yaml:"reportIntervalSec"` // continuous-mode snapshot cadence
}

// Target is the whole environment description.
type Target struct {
	Name          string   `yaml:"name"`
	ChainID       uint64   `yaml:"chainId"` // EVM eip155 chain id
	CosmosChainID string   `yaml:"cosmosChainId"`
	Nodes         []Node   `yaml:"nodes"`
	Funding       Funding  `yaml:"funding"`
	Workload      Workload `yaml:"workload"`
	Observe       Observe  `yaml:"observe"`
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
	// durationSec is intentionally NOT defaulted: <= 0 means continuous.
}

func (t *Target) validate() error {
	if t.ChainID == 0 {
		return fmt.Errorf("chainId must be set (EVM eip155 id)")
	}
	if len(t.Nodes) == 0 {
		return fmt.Errorf("at least one node is required")
	}
	for i, n := range t.Nodes {
		if n.JSONRPC == "" {
			return fmt.Errorf("node[%d] %q missing jsonrpc", i, n.Name)
		}
	}
	return nil
}

// PrimaryJSONRPC returns the JSON-RPC endpoint used for sending load txs
// (prefers a fullnode, else the first node).
func (t *Target) PrimaryJSONRPC() string {
	for _, n := range t.Nodes {
		if n.Role == RoleFullnode {
			return n.JSONRPC
		}
	}
	return t.Nodes[0].JSONRPC
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
