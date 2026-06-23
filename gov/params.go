// Package gov registers Tx-Type/VIP lanes via the gov precompile (mirroring
// scripts/stable-client-txtrigger gov flow) and reads back the on-chain params
// the collector classifies against.
package gov

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// QueryParams reads the current x/stable params over cosmos gRPC.
func QueryParams(ctx context.Context, grpcEndpoint string) (*stabletypes.Params, error) {
	if grpcEndpoint == "" {
		return nil, fmt.Errorf("no gRPC endpoint configured (set node.grpc to query params)")
	}
	conn, err := grpc.NewClient(grpcEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial gRPC %s: %w", grpcEndpoint, err)
	}
	defer conn.Close()

	qc := stabletypes.NewQueryClient(conn)
	resp, err := qc.Params(ctx, &stabletypes.QueryParamsRequest{})
	if err != nil {
		return nil, fmt.Errorf("query stable params: %w", err)
	}
	return &resp.Params, nil
}

// hasLanes reports whether p contains all lane ids in want.
func hasLanes(p *stabletypes.Params, wantVip []int32, wantTxType []int32) bool {
	have := make(map[int32]struct{})
	for _, l := range p.VipLanes {
		have[l.Id] = struct{}{}
	}
	for _, l := range p.TxTypeLanes {
		have[l.Id] = struct{}{}
	}
	for _, id := range wantVip {
		if _, ok := have[id]; !ok {
			return false
		}
	}
	for _, id := range wantTxType {
		if _, ok := have[id]; !ok {
			return false
		}
	}
	return true
}
