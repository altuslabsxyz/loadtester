// Package laneplan derives the Tx-Type / VIP lane configuration from a
// deployment. It is the single source of truth shared by:
//   - gov:       registers these lanes via MsgUpdateParams.
//   - collector: classifies included txs against these lanes (quota check).
//   - workload:  labels each sent tx with the lane it is expected to land in.
package laneplan

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/stablelabs/loadtester/config"
	"github.com/stablelabs/loadtester/deployment"
	"github.com/stablelabs/loadtester/workload"
	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// LaneNormalID mirrors blockspace.LaneNormal (max int32). Txs matching no lane
// fall here. Used as the "expected lane" for unmatched workloads.
const LaneNormalID int32 = 1<<31 - 1

// Lane ids used by the plan. Lower id = higher match priority.
const (
	LaneIDERC20Transfer int32 = 10
	LaneIDUniswapSwap   int32 = 20
	LaneIDVIP           int32 = 1
)

// Plan is the resolved lane configuration plus the workload->lane mapping.
type Plan struct {
	MaxBlockspaceGasWeight uint32
	VipLanes               []stabletypes.VipLaneParam
	TxTypeLanes            []stabletypes.TxTypeLaneParam

	// ExpectedLane maps a workload kind to the lane id its txs should land in.
	ExpectedLane map[workload.Kind]int32
}

// Build constructs the default lane plan from a deployment and parsed ABIs.
//
// Defaults:
//   - reserved pool = 50% of max block gas (MaxBlockspaceGasWeight=50)
//   - erc20-transfer lane: to=tokens[0], method=transfer selector, weight 30
//   - uniswap-swap lane:   to=callee,    method=swapExact0For1,    weight 20
//   - vip lane:            weight 20
//
// weight sum (30+20+20=70) stays <= 100 as required by params validation.
func Build(d *deployment.Deployment, abis *workload.ABIs) *Plan {
	p := &Plan{
		MaxBlockspaceGasWeight: 50,
		ExpectedLane: map[workload.Kind]int32{
			workload.KindValue:        LaneNormalID,
			workload.KindBump:         LaneNormalID,
			workload.KindSelfDestruct: LaneNormalID,
		},
	}

	// VIP lane (matched by nonce-key VIP bit, not by tx fields).
	p.VipLanes = append(p.VipLanes, stabletypes.VipLaneParam{
		Id:     LaneIDVIP,
		Name:   "lt-vip",
		Weight: 20,
	})
	p.ExpectedLane[workload.KindVIP] = LaneIDVIP

	transferSel := workload.Selector(abis.ERC20, "transfer")
	swapSel := workload.Selector(abis.Callee, "swapExact0For1")

	// erc20-transfer lane keyed on the first test token + transfer selector.
	if len(d.Tokens) > 0 {
		p.TxTypeLanes = append(p.TxTypeLanes, stabletypes.TxTypeLaneParam{
			Id:      LaneIDERC20Transfer,
			Name:    "lt-erc20-transfer",
			Weight:  30,
			ToAddrs: []string{d.Tokens[0].Address},
			Methods: [][]byte{transferSel[:]},
			TxTypes: []stabletypes.TxFormat{stabletypes.TxFormat_TX_FORMAT_DYNAMIC_FEE},
		})
		p.ExpectedLane[workload.KindERC20Transfer] = LaneIDERC20Transfer
	}

	// uniswap-swap lane keyed on the callee + swap selector.
	if d.Callee != "" {
		p.TxTypeLanes = append(p.TxTypeLanes, stabletypes.TxTypeLaneParam{
			Id:      LaneIDUniswapSwap,
			Name:    "lt-uniswap-swap",
			Weight:  20,
			ToAddrs: []string{d.Callee},
			Methods: [][]byte{swapSel[:]},
			TxTypes: []stabletypes.TxFormat{stabletypes.TxFormat_TX_FORMAT_DYNAMIC_FEE},
		})
		p.ExpectedLane[workload.KindSwap] = LaneIDUniswapSwap
	}

	return p
}

// Params assembles the stable module Params for this plan.
func (p *Plan) Params() stabletypes.Params {
	return stabletypes.Params{
		EnableGasWaiver:        false,
		VipLanes:               p.VipLanes,
		TxTypeLanes:            p.TxTypeLanes,
		MaxBlockspaceGasWeight: p.MaxBlockspaceGasWeight,
	}
}

// BuildFromConfig builds a Plan from config-defined lanes (instead of the
// preset). Addresses accept "@name" refs resolved via the deployment registry;
// methods are 4-byte hex selectors. ExpectedLane is left empty and derived at
// runtime by classifying each workload's sample tx against the on-chain params.
func BuildFromConfig(bs config.Blockspace, d *deployment.Deployment) (*Plan, error) {
	p := &Plan{
		MaxBlockspaceGasWeight: bs.MaxBlockspaceGasWeight,
		ExpectedLane:           map[workload.Kind]int32{},
	}
	for _, l := range bs.Lanes {
		if l.VIP {
			p.VipLanes = append(p.VipLanes, stabletypes.VipLaneParam{
				Id: l.ID, Name: l.Name, Weight: l.Weight,
			})
			continue
		}
		toAddrs, err := resolveAddrs(d, l.ToAddrs)
		if err != nil {
			return nil, fmt.Errorf("lane %q toAddrs: %w", l.Name, err)
		}
		senders, err := resolveAddrs(d, l.Senders)
		if err != nil {
			return nil, fmt.Errorf("lane %q senders: %w", l.Name, err)
		}
		methods, err := parseSelectors(l.Methods)
		if err != nil {
			return nil, fmt.Errorf("lane %q methods: %w", l.Name, err)
		}
		txTypes, err := parseTxFormats(l.TxTypes)
		if err != nil {
			return nil, fmt.Errorf("lane %q txTypes: %w", l.Name, err)
		}
		p.TxTypeLanes = append(p.TxTypeLanes, stabletypes.TxTypeLaneParam{
			Id:         l.ID,
			Name:       l.Name,
			Weight:     l.Weight,
			ToAddrs:    toAddrs,
			Methods:    methods,
			TxTypes:    txTypes,
			NonceKeys:  l.NonceKeys,
			Senders:    senders,
			NoOverflow: l.NoOverflow,
		})
	}
	// VIP workload's nonce-key lane id must be known up front to BUILD its txs
	// (cannot be derived by classifying, which is circular). Take the first VIP
	// lane. Other kinds' expected lane is derived later via the classifier.
	for _, v := range p.VipLanes {
		p.ExpectedLane[workload.KindVIP] = v.Id
		break
	}
	return p, nil
}

func resolveAddrs(d *deployment.Deployment, in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	for _, s := range in {
		r, ok := d.ResolveAddr(s)
		if !ok || r == "" {
			return nil, fmt.Errorf("unresolved address ref %q", s)
		}
		out = append(out, r)
	}
	return out, nil
}

func parseSelectors(in []string) ([][]byte, error) {
	out := make([][]byte, 0, len(in))
	for _, s := range in {
		b, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(s), "0x"))
		if err != nil {
			return nil, fmt.Errorf("selector %q: %w", s, err)
		}
		if len(b) != 4 {
			return nil, fmt.Errorf("selector %q must be 4 bytes, got %d", s, len(b))
		}
		out = append(out, b)
	}
	return out, nil
}

func parseTxFormats(in []string) ([]stabletypes.TxFormat, error) {
	m := map[string]stabletypes.TxFormat{
		"LEGACY":      stabletypes.TxFormat_TX_FORMAT_LEGACY,
		"ACCESS_LIST": stabletypes.TxFormat_TX_FORMAT_ACCESS_LIST,
		"DYNAMIC_FEE": stabletypes.TxFormat_TX_FORMAT_DYNAMIC_FEE,
		"SET_CODE":    stabletypes.TxFormat_TX_FORMAT_SET_CODE,
		"TWO_D_NONCE": stabletypes.TxFormat_TX_FORMAT_TWO_D_NONCE,
	}
	out := make([]stabletypes.TxFormat, 0, len(in))
	for _, s := range in {
		f, ok := m[strings.ToUpper(strings.TrimSpace(s))]
		if !ok {
			return nil, fmt.Errorf("unknown txType %q", s)
		}
		out = append(out, f)
	}
	return out, nil
}
