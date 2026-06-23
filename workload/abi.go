package workload

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// Minimal ABIs for the methods the load-tester drives. Kept inline so the Go
// harness has no dependency on TS build artifacts; signatures mirror the
// uniswap-v3-core test contracts and the Destructible helper.
const (
	erc20ABIJSON = `[
	  {"name":"transfer","type":"function","inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"outputs":[{"type":"bool"}]},
	  {"name":"approve","type":"function","inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"outputs":[{"type":"bool"}]},
	  {"name":"mint","type":"function","inputs":[{"name":"to","type":"address"},{"name":"amount","type":"uint256"}],"outputs":[]},
	  {"name":"balanceOf","type":"function","stateMutability":"view","inputs":[{"name":"owner","type":"address"}],"outputs":[{"type":"uint256"}]}
	]`

	calleeABIJSON = `[
	  {"name":"swapExact0For1","type":"function","inputs":[{"name":"pool","type":"address"},{"name":"amount0In","type":"uint256"},{"name":"recipient","type":"address"},{"name":"sqrtPriceLimitX96","type":"uint160"}],"outputs":[]},
	  {"name":"mint","type":"function","inputs":[{"name":"pool","type":"address"},{"name":"recipient","type":"address"},{"name":"tickLower","type":"int24"},{"name":"tickUpper","type":"int24"},{"name":"amount","type":"uint128"}],"outputs":[]}
	]`

	// Destructible helper (deployed by the TS deployer). bump writes a storage
	// slot (same-slot contention across accounts); spawnAndDestroy CREATEs N
	// children that SELFDESTRUCT in the same call.
	destructibleABIJSON = `[
	  {"name":"bump","type":"function","inputs":[],"outputs":[]},
	  {"name":"spawnAndDestroy","type":"function","inputs":[{"name":"count","type":"uint256"}],"outputs":[]}
	]`
)

// ABIs holds the parsed ABIs and is shared (read-only) across workers.
type ABIs struct {
	ERC20        abi.ABI
	Callee       abi.ABI
	Destructible abi.ABI
}

// LoadABIs parses the inline ABIs once.
func LoadABIs() (*ABIs, error) {
	erc20, err := abi.JSON(strings.NewReader(erc20ABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse erc20 abi: %w", err)
	}
	callee, err := abi.JSON(strings.NewReader(calleeABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse callee abi: %w", err)
	}
	destructible, err := abi.JSON(strings.NewReader(destructibleABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse destructible abi: %w", err)
	}
	return &ABIs{ERC20: erc20, Callee: callee, Destructible: destructible}, nil
}

// Selector returns the 4-byte selector of a method in the given ABI.
func Selector(a abi.ABI, method string) [4]byte {
	var s [4]byte
	m, ok := a.Methods[method]
	if !ok {
		return s
	}
	copy(s[:], m.ID)
	return s
}

// pack is a small wrapper returning calldata for a method.
func pack(a abi.ABI, method string, args ...any) ([]byte, error) {
	data, err := a.Pack(method, args...)
	if err != nil {
		return nil, fmt.Errorf("pack %s: %w", method, err)
	}
	return data, nil
}

// PackTransfer builds calldata for ERC20 transfer(to, amount).
func (a *ABIs) PackTransfer(to common.Address, amount *big.Int) ([]byte, error) {
	return pack(a.ERC20, "transfer", to, amount)
}

// PackApprove builds calldata for ERC20 approve(spender, amount).
func (a *ABIs) PackApprove(spender common.Address, amount *big.Int) ([]byte, error) {
	return pack(a.ERC20, "approve", spender, amount)
}

// PackMintERC20 builds calldata for TestERC20 mint(to, amount).
func (a *ABIs) PackMintERC20(to common.Address, amount *big.Int) ([]byte, error) {
	return pack(a.ERC20, "mint", to, amount)
}

// PackSwapExact0For1 builds calldata for the callee swap helper.
func (a *ABIs) PackSwapExact0For1(pool common.Address, amount0In *big.Int, recipient common.Address, sqrtPriceLimit *big.Int) ([]byte, error) {
	return pack(a.Callee, "swapExact0For1", pool, amount0In, recipient, sqrtPriceLimit)
}

// PackBump builds calldata for Destructible.bump().
func (a *ABIs) PackBump() ([]byte, error) {
	return pack(a.Destructible, "bump")
}

// PackSpawnAndDestroy builds calldata for Destructible.spawnAndDestroy(count).
func (a *ABIs) PackSpawnAndDestroy(count *big.Int) ([]byte, error) {
	return pack(a.Destructible, "spawnAndDestroy", count)
}
