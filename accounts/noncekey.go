package accounts

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// NonceKeyPrecompile is the stable 2D-nonce query precompile
// (stable/precompiles/precompiles.go). getNonce(account, nonceKey) returns the
// NEXT sequence to use for that (account, key) pair - the ante stores
// sequence+1 after each tx - so the result is the value to set as tx.Nonce.
// eth_getTransactionCount cannot report this (it only knows nonce key 0).
var NonceKeyPrecompile = common.HexToAddress("0x0000000000000000000000000000000000002D2D")

const noncekeyABIJSON = `[{"type":"function","name":"getNonce","stateMutability":"view",` +
	`"inputs":[{"name":"account","type":"address"},{"name":"nonceKey","type":"uint64"}],` +
	`"outputs":[{"name":"nonce","type":"uint64"}]}]`

var noncekeyABI = mustParseABI(noncekeyABIJSON)

func mustParseABI(s string) abi.ABI {
	a, err := abi.JSON(strings.NewReader(s))
	if err != nil {
		panic(fmt.Sprintf("parse noncekey abi: %v", err))
	}
	return a
}

// Nonce2D queries the 2D-nonce precompile for the next nonce of (addr, nonceKey)
// over client. Use it to seed / resync a non-zero nonce key (e.g. VIP), which
// eth_getTransactionCount cannot report.
func Nonce2D(ctx context.Context, client *ethclient.Client, addr common.Address, nonceKey uint64) (uint64, error) {
	data, err := noncekeyABI.Pack("getNonce", addr, nonceKey)
	if err != nil {
		return 0, fmt.Errorf("pack getNonce: %w", err)
	}
	out, err := client.CallContract(ctx, ethereum.CallMsg{To: &NonceKeyPrecompile, Data: data}, nil)
	if err != nil {
		return 0, fmt.Errorf("call noncekey precompile: %w", err)
	}
	vals, err := noncekeyABI.Unpack("getNonce", out)
	if err != nil || len(vals) == 0 {
		return 0, fmt.Errorf("unpack getNonce: %w", err)
	}
	switch v := vals[0].(type) {
	case uint64:
		return v, nil
	case *big.Int:
		return v.Uint64(), nil
	default:
		return 0, fmt.Errorf("unexpected getNonce return type %T", vals[0])
	}
}
