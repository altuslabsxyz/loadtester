// Package laneplan derives the Tx-Type / VIP lane configuration from a
// deployment. It is the single source of truth shared by:
//   - gov:       registers these lanes via MsgUpdateParams.
//   - collector: classifies included txs against these lanes (quota check).
//   - workload:  labels each sent tx with the lane it is expected to land in.
package laneplan

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/stablelabs/loadtester/config"
	"github.com/stablelabs/loadtester/deployment"
	"github.com/stablelabs/loadtester/workload"
	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// Reconcile compares the (resolved) declared plan against the on-chain params.
// It returns lane ids declared but ABSENT on-chain (missing) and ids present but
// whose effective params DIFFER (mismatched: weight or, for tx-type lanes, the
// matchers). The lane Name is ignored (cosmetic). This is what lets the report
// say a preconfigured target's declared lanes truly match what's registered -
// matching on id alone would miss a wrong weight (=> wrong quota) or matcher.
func Reconcile(p *Plan, onchain *stabletypes.Params) (missing, mismatched []int32) {
	vip := make(map[int32]stabletypes.VipLaneParam, len(onchain.VipLanes))
	for _, l := range onchain.VipLanes {
		vip[l.Id] = l
	}
	txt := make(map[int32]stabletypes.TxTypeLaneParam, len(onchain.TxTypeLanes))
	for _, l := range onchain.TxTypeLanes {
		txt[l.Id] = l
	}
	for _, l := range p.VipLanes {
		oc, ok := vip[l.Id]
		switch {
		case !ok:
			missing = append(missing, l.Id)
		case oc.Weight != l.Weight:
			mismatched = append(mismatched, l.Id)
		}
	}
	for _, l := range p.TxTypeLanes {
		oc, ok := txt[l.Id]
		if !ok {
			missing = append(missing, l.Id)
			continue
		}
		if !sameTxTypeLane(l, oc) {
			mismatched = append(mismatched, l.Id)
		}
	}
	return missing, mismatched
}

// sameTxTypeLane compares the FUNCTIONAL params of two tx-type lanes (ignoring
// the cosmetic Name). Matcher slices are OR-combined and stored verbatim by the
// chain, so the comparison is order-independent; addresses are normalized so
// checksum-vs-lowercase doesn't read as a difference. (A naive reflect.DeepEqual
// would falsely flag a correct config whose address casing or matcher order
// differs from the on-chain encoding.)
func sameTxTypeLane(a, b stabletypes.TxTypeLaneParam) bool {
	if a.Weight != b.Weight || a.NoOverflow != b.NoOverflow {
		return false
	}
	return sameAddrSet(a.ToAddrs, b.ToAddrs) &&
		sameAddrSet(a.Senders, b.Senders) &&
		sameStrSet(methodHexes(a.Methods), methodHexes(b.Methods)) &&
		sameTxFormatSet(a.TxTypes, b.TxTypes) &&
		sameU64Set(a.NonceKeys, b.NonceKeys)
}

func sameAddrSet(a, b []string) bool {
	// common.HexToAddress is silent/lossy (invalid/short/over-length strings
	// collapse to zero/truncated/padded with no error), so a typo'd DECLARED
	// address could otherwise false-match. Reject any non-address up front: an
	// unparseable address cannot legitimately equal a validated on-chain one.
	na, okA := normAddrs(a)
	nb, okB := normAddrs(b)
	if !okA || !okB {
		return false
	}
	return sameStrSet(na, nb)
}

func normAddrs(in []string) ([]string, bool) {
	out := make([]string, len(in))
	for i, s := range in {
		if !common.IsHexAddress(s) {
			return nil, false
		}
		out[i] = common.HexToAddress(s).Hex() // canonical checksum form
	}
	return out, true
}

func methodHexes(in [][]byte) []string {
	out := make([]string, len(in))
	for i, m := range in {
		out[i] = hex.EncodeToString(m)
	}
	return out
}

func sameStrSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameTxFormatSet(a, b []stabletypes.TxFormat) bool {
	if len(a) != len(b) {
		return false
	}
	ia := make([]int, len(a))
	for i, v := range a {
		ia[i] = int(v)
	}
	ib := make([]int, len(b))
	for i, v := range b {
		ib[i] = int(v)
	}
	sort.Ints(ia)
	sort.Ints(ib)
	for i := range ia {
		if ia[i] != ib[i] {
			return false
		}
	}
	return true
}

func sameU64Set(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	a = append([]uint64(nil), a...)
	b = append([]uint64(nil), b...)
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

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
		// Pre-seed the non-matched kinds to LaneNormal (mirrors Build), so a kind
		// whose expected lane isn't derived later never defaults to the int32 zero
		// value (which would mis-attribute every such tx to a bogus "lane 0").
		ExpectedLane: map[workload.Kind]int32{
			workload.KindValue:        LaneNormalID,
			workload.KindBump:         LaneNormalID,
			workload.KindSelfDestruct: LaneNormalID,
		},
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
