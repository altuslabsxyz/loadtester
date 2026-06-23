package collector

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"

	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// LaneViolation is a block where a lane's attributed gas exceeded its quota.
type LaneViolation struct {
	Height   uint64
	LaneID   int32
	LaneName string
	GasUsed  uint64
	Quota    uint64
}

// LaneResult is the lane-attribution collector output.
type LaneResult struct {
	BlocksObserved int
	// PeakLaneGas is the max attributed gas-limit sum seen for each lane in a block.
	PeakLaneGas map[int32]uint64
	Quota       map[int32]uint64
	LaneNames   map[int32]string
	// Violations holds the most recent over-quota attributions (bounded);
	// ViolationCount is the cumulative total (continuous-mode safe).
	Violations     []LaneViolation
	ViolationCount int
}

// maxLaneViolations caps retained violation samples (continuous-mode safety).
const maxLaneViolations = 512

// LaneCollector attributes included txs to lanes and checks per-lane quota.
type LaneCollector struct {
	client      *ethclient.Client
	classifier  *Classifier
	params      *stabletypes.Params
	maxBlockGas uint64

	mu        sync.Mutex
	res       LaneResult
	lastBlock uint64
}

func NewLaneCollector(client *ethclient.Client, classifier *Classifier, params *stabletypes.Params, maxBlockGas uint64) *LaneCollector {
	names := map[int32]string{laneNormalID: "normal"}
	quota := map[int32]uint64{}
	for _, v := range params.VipLanes {
		names[v.Id] = v.Name
		quota[v.Id] = MaxGasForLane(params, v.Id, maxBlockGas)
	}
	for _, t := range params.TxTypeLanes {
		names[t.Id] = t.Name
		quota[t.Id] = MaxGasForLane(params, t.Id, maxBlockGas)
	}
	return &LaneCollector{
		client:      client,
		classifier:  classifier,
		params:      params,
		maxBlockGas: maxBlockGas,
		res: LaneResult{
			PeakLaneGas: map[int32]uint64{},
			Quota:       quota,
			LaneNames:   names,
		},
	}
}

// Run polls new blocks until ctx is done, attributing gas per lane.
func (lc *LaneCollector) Run(ctx context.Context, pollInterval time.Duration) {
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			lc.poll(ctx)
		}
	}
}

func (lc *LaneCollector) poll(ctx context.Context) {
	head, err := lc.client.BlockNumber(ctx)
	if err != nil {
		return
	}
	for h := lc.lastBlock + 1; h <= head; h++ {
		lc.processBlock(ctx, h)
		lc.lastBlock = h
	}
}

func (lc *LaneCollector) processBlock(ctx context.Context, height uint64) {
	blk, err := lc.client.BlockByNumber(ctx, new(big.Int).SetUint64(height))
	if err != nil {
		return
	}
	// The chain enforces per-lane quota against each tx's gas LIMIT (GetGas) at
	// proposal time (app/blockspace/selector.go), NOT the receipt gasUsed. Mirror
	// that: sum tx.Gas() so the comparison against MaxGasForLane is unit-correct.
	perLane := map[int32]uint64{}
	for _, tx := range blk.Transactions() {
		laneID := lc.classifier.PrimaryLane(tx)
		perLane[laneID] += tx.Gas()
	}

	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.res.BlocksObserved++
	for laneID, used := range perLane {
		if used > lc.res.PeakLaneGas[laneID] {
			lc.res.PeakLaneGas[laneID] = used
		}
		quota := lc.res.Quota[laneID]
		if quota > 0 && used > quota {
			lc.res.ViolationCount++
			lc.res.Violations = append(lc.res.Violations, LaneViolation{
				Height:   height,
				LaneID:   laneID,
				LaneName: lc.res.LaneNames[laneID],
				GasUsed:  used,
				Quota:    quota,
			})
			if n := len(lc.res.Violations); n > maxLaneViolations {
				lc.res.Violations = append(lc.res.Violations[:0], lc.res.Violations[n-maxLaneViolations:]...)
			}
		}
	}
}

// Result returns a snapshot of the collected lane data.
func (lc *LaneCollector) Result() LaneResult {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	cp := LaneResult{
		BlocksObserved: lc.res.BlocksObserved,
		PeakLaneGas:    map[int32]uint64{},
		Quota:          map[int32]uint64{},
		LaneNames:      map[int32]string{},
		Violations:     append([]LaneViolation(nil), lc.res.Violations...),
		ViolationCount: lc.res.ViolationCount,
	}
	for k, v := range lc.res.PeakLaneGas {
		cp.PeakLaneGas[k] = v
	}
	for k, v := range lc.res.Quota {
		cp.Quota[k] = v
	}
	for k, v := range lc.res.LaneNames {
		cp.LaneNames[k] = v
	}
	return cp
}
