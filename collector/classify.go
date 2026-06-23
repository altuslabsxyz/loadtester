package collector

import (
	"bytes"
	"math"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// laneNormalID mirrors blockspace.LaneNormal.
const laneNormalID int32 = 1<<31 - 1

// Classifier reconstructs the chain's lane matching (mirror of
// app/blockspace/selector.go ClassifyTx) from the on-chain params, so the
// collector can attribute included txs to lanes independently of the chain.
//
// NOTE: this mirrors selector.go as of the spec date. If the chain's matching
// logic changes, update this in lockstep (see spec §5.1 drift caveat).
type Classifier struct {
	params *stabletypes.Params
	signer ethtypes.Signer
}

func NewClassifier(params *stabletypes.Params, signer ethtypes.Signer) *Classifier {
	return &Classifier{params: params, signer: signer}
}

// PrimaryLane returns the highest-priority lane the tx matches: VIP by nonce-key
// bit, else the lowest-id matching tx-type lane, else LaneNormal. This is the
// lane the proposer charges first; overflow may move a tx to a lower-priority
// lane, so per-lane sums built from this are an upper bound on primary usage.
func (c *Classifier) PrimaryLane(tx *ethtypes.Transaction) int32 {
	if c.params == nil {
		return laneNormalID
	}
	nonceKey := tx.NonceKey()
	if isVip(nonceKey) {
		vipID := int32(nonceKey & stabletypes.VipMask)
		for _, v := range c.params.VipLanes {
			if v.Id == vipID {
				return v.Id
			}
		}
		// VIP bit set but no matching lane: fall through to tx-type matching.
	}

	// Primary lane = lowest-id matching tx-type lane (the lane the proposer
	// charges first). For an overflow lane, excess txs cascade to lower-priority
	// lanes, so a per-lane sum from this attribution is an UPPER BOUND. For a
	// NoOverflow (hard-cap) lane the chain skips/rejects instead of cascading, so
	// such a lane can never exceed quota on-chain - a reported "violation" there
	// would be spurious. The report defers to proposer skip-logs for the
	// authoritative enforcement signal either way.
	best := laneNormalID
	bestSet := false
	for _, lane := range c.params.TxTypeLanes {
		if !c.matches(tx, nonceKey, lane) {
			continue
		}
		if !bestSet || lane.Id < best {
			best = lane.Id
			bestSet = true
		}
	}
	return best
}

func isVip(nonceKey uint64) bool {
	return nonceKey != math.MaxUint64 && nonceKey&stabletypes.VipFlag != 0
}

func (c *Classifier) matches(tx *ethtypes.Transaction, nonceKey uint64, lane stabletypes.TxTypeLaneParam) bool {
	to := tx.To()
	data := tx.Data()

	if !matchToAddr(to, lane.ToAddrs) {
		return false
	}
	if !matchMethod(data, lane.Methods) {
		return false
	}
	if !matchTxType(tx, lane.TxTypes) {
		return false
	}
	if !matchNonceKey(nonceKey, lane.NonceKeys) {
		return false
	}
	if len(lane.Senders) > 0 && !c.matchSender(tx, lane.Senders) {
		return false
	}
	return true
}

func matchToAddr(to *common.Address, want []string) bool {
	if len(want) == 0 {
		return true
	}
	if to == nil {
		return false
	}
	for _, w := range want {
		if common.IsHexAddress(w) && common.HexToAddress(w) == *to {
			return true
		}
	}
	return false
}

func matchMethod(data []byte, want [][]byte) bool {
	if len(want) == 0 {
		return true
	}
	for _, sel := range want {
		if len(sel) == 0 || len(data) < len(sel) {
			continue
		}
		if bytes.Equal(data[:len(sel)], sel) {
			return true
		}
	}
	return false
}

func matchTxType(tx *ethtypes.Transaction, want []stabletypes.TxFormat) bool {
	if len(want) == 0 {
		return true
	}
	got := txFormatOf(tx)
	for _, w := range want {
		if w == stabletypes.TxFormat_TX_FORMAT_UNSPECIFIED || w == got {
			return true
		}
	}
	return false
}

func matchNonceKey(nonceKey uint64, want []uint64) bool {
	if len(want) == 0 {
		return true
	}
	for _, k := range want {
		if k == 0 || k == nonceKey {
			return true
		}
	}
	return false
}

func (c *Classifier) matchSender(tx *ethtypes.Transaction, want []string) bool {
	from, err := ethtypes.Sender(c.signer, tx)
	if err != nil {
		return false
	}
	for _, w := range want {
		if common.IsHexAddress(w) && common.HexToAddress(w) == from {
			return true
		}
	}
	return false
}

func txFormatOf(tx *ethtypes.Transaction) stabletypes.TxFormat {
	switch tx.Type() {
	case ethtypes.LegacyTxType:
		return stabletypes.TxFormat_TX_FORMAT_LEGACY
	case ethtypes.AccessListTxType:
		return stabletypes.TxFormat_TX_FORMAT_ACCESS_LIST
	case ethtypes.DynamicFeeTxType:
		return stabletypes.TxFormat_TX_FORMAT_DYNAMIC_FEE
	case ethtypes.SetCodeTxType:
		return stabletypes.TxFormat_TX_FORMAT_SET_CODE
	case ethtypes.CustomTxType:
		return stabletypes.TxFormat_TX_FORMAT_TWO_D_NONCE
	default:
		return stabletypes.TxFormat_TX_FORMAT_UNSPECIFIED
	}
}

// MaxGasForLane mirrors blockspace.Lane.MaxGasForLane.
func MaxGasForLane(params *stabletypes.Params, laneID int32, maxBlockGas uint64) uint64 {
	if params == nil || laneID == laneNormalID || maxBlockGas == 0 || params.MaxBlockspaceGasWeight == 0 {
		return 0
	}
	weight := weightForLane(params, laneID)
	if weight == 0 {
		return 0
	}
	reserved := maxBlockGas * uint64(params.MaxBlockspaceGasWeight) / 100
	return reserved * uint64(weight) / 100
}

func weightForLane(params *stabletypes.Params, laneID int32) uint32 {
	for _, v := range params.VipLanes {
		if v.Id == laneID {
			return v.Weight
		}
	}
	for _, t := range params.TxTypeLanes {
		if t.Id == laneID {
			return t.Weight
		}
	}
	return 0
}
