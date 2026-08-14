package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// cometHTTP is a dedicated client with a finite timeout. http.DefaultClient has
// NO timeout: against a remote/flaky CometBFT RPC behind a load balancer, a
// connection that is accepted but never answered would block the calling
// goroutine until the root context is cancelled (i.e. until Ctrl+C). A bounded
// per-request timeout keeps collectors and the drain loop live on a testnet.
var cometHTTP = &http.Client{Timeout: 10 * time.Second}

// cometResult unwraps a CometBFT JSON-RPC 2.0 response.
type cometResult struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Message string `json:"message"`
		Data    string `json:"data"`
	} `json:"error"`
}

// cometGet issues a GET against a CometBFT RPC endpoint path with query params.
func cometGet(ctx context.Context, base, path string, query map[string]string, out any) error {
	u := strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
	if len(query) > 0 {
		parts := make([]string, 0, len(query))
		for k, v := range query {
			parts = append(parts, k+"="+v)
		}
		u += "?" + strings.Join(parts, "&")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := cometHTTP.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		resp.Body.Close()
		return fmt.Errorf("comet rpc %s: HTTP %d", path, resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var cr cometResult
	if err := json.Unmarshal(body, &cr); err != nil {
		return fmt.Errorf("decode %s: %w", u, err)
	}
	if cr.Error != nil {
		return fmt.Errorf("comet rpc %s error: %s %s", path, cr.Error.Message, cr.Error.Data)
	}
	return json.Unmarshal(cr.Result, out)
}

// NumUnconfirmedTxs returns the CList mempool size (n_txs) at a node.
func NumUnconfirmedTxs(ctx context.Context, cometRPC string) (int, error) {
	var r struct {
		NTxs  string `json:"n_txs"`
		Total string `json:"total"`
	}
	if err := cometGet(ctx, cometRPC, "num_unconfirmed_txs", nil, &r); err != nil {
		return 0, err
	}
	v := r.NTxs
	if v == "" {
		v = r.Total
	}
	n, _ := strconv.Atoi(v)
	return n, nil
}

// BlockAppHash returns (appHash, height) for a height (0 = latest).
func BlockAppHash(ctx context.Context, cometRPC string, height int64) (string, int64, error) {
	q := map[string]string{}
	if height > 0 {
		q["height"] = strconv.FormatInt(height, 10)
	}
	var r struct {
		Block struct {
			Header struct {
				Height  string `json:"height"`
				AppHash string `json:"app_hash"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := cometGet(ctx, cometRPC, "block", q, &r); err != nil {
		return "", 0, err
	}
	h, _ := strconv.ParseInt(r.Block.Header.Height, 10, 64)
	return r.Block.Header.AppHash, h, nil
}

// StatusHeight returns the latest block height at a node.
func StatusHeight(ctx context.Context, cometRPC string) (int64, error) {
	var r struct {
		SyncInfo struct {
			LatestBlockHeight string `json:"latest_block_height"`
		} `json:"sync_info"`
	}
	if err := cometGet(ctx, cometRPC, "status", nil, &r); err != nil {
		return 0, err
	}
	h, _ := strconv.ParseInt(r.SyncInfo.LatestBlockHeight, 10, 64)
	return h, nil
}

// MaxBlockGas reads consensus_params block.max_gas (returns 0 if unlimited/-1).
func MaxBlockGas(ctx context.Context, cometRPC string) (uint64, error) {
	var r struct {
		ConsensusParams struct {
			Block struct {
				MaxGas string `json:"max_gas"`
			} `json:"block"`
		} `json:"consensus_params"`
	}
	if err := cometGet(ctx, cometRPC, "consensus_params", nil, &r); err != nil {
		return 0, err
	}
	g, _ := strconv.ParseInt(r.ConsensusParams.Block.MaxGas, 10, 64)
	if g < 0 {
		return 0, nil
	}
	return uint64(g), nil
}

// CommittedTxCounter accumulates the number of transactions the chain has
// COMMITTED, read from a node that is caught up (a validator).
//
// The load driver needs this to know its FRESH supply - the txs it has sent
// that are not yet in any block - which is the only part of a mempool the tx
// provider can actually serve. Depth alone conflates fresh txs with ones the
// validators already committed and the provider node has not pruned yet,
// and those two behave completely differently: the stale part is invisible to
// the proposer until it is pruned, and then becomes servable all at once.
type CommittedTxCounter struct {
	cometRPC string
	last     int64  // highest height already counted
	total    uint64 // cumulative txs over counted heights
}

func NewCommittedTxCounter(cometRPC string) *CommittedTxCounter {
	return &CommittedTxCounter{cometRPC: cometRPC}
}

// blockchainMeta is the subset of /blockchain this needs.
type blockchainMeta struct {
	LastHeight string `json:"last_height"`
	BlockMetas []struct {
		NumTxs string `json:"num_txs"`
		Header struct {
			Height string `json:"height"`
		} `json:"header"`
	} `json:"block_metas"`
}

// Total advances the counter to the chain head and returns the cumulative
// committed tx count. The first call establishes the baseline and returns 0.
//
// /blockchain serves at most 20 metas per call, so a caller that falls far
// behind is caught up over several calls rather than in one huge fetch.
func (c *CommittedTxCounter) Total(ctx context.Context) (uint64, error) {
	var r blockchainMeta
	if err := cometGet(ctx, c.cometRPC, "blockchain", nil, &r); err != nil {
		return c.total, err
	}
	head, err := strconv.ParseInt(r.LastHeight, 10, 64)
	if err != nil || head <= 0 {
		return c.total, fmt.Errorf("blockchain: bad last_height %q", r.LastHeight)
	}
	if c.last == 0 { // baseline: count from here on, not from genesis
		c.last = head
		return 0, nil
	}
	if head <= c.last {
		return c.total, nil
	}
	// Walk forward in 20-height pages until the head is reached.
	for c.last < head {
		lo, hi := c.last+1, c.last+20
		if hi > head {
			hi = head
		}
		var page blockchainMeta
		q := map[string]string{
			"minHeight": strconv.FormatInt(lo, 10),
			"maxHeight": strconv.FormatInt(hi, 10),
		}
		if err := cometGet(ctx, c.cometRPC, "blockchain", q, &page); err != nil {
			return c.total, err
		}
		if len(page.BlockMetas) == 0 {
			break
		}
		for _, m := range page.BlockMetas {
			h, herr := strconv.ParseInt(m.Header.Height, 10, 64)
			if herr != nil || h <= c.last {
				continue
			}
			n, _ := strconv.ParseUint(m.NumTxs, 10, 64)
			c.total += n
		}
		c.last = hi
	}
	return c.total, nil
}
