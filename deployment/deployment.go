// Package deployment is the boundary contract between the TS deployer and the
// Go harness. The TS deployer writes deployment.json; the Go harness reads it.
package deployment

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
)

// Token is a deployed ERC20 test token.
type Token struct {
	Symbol  string `json:"symbol"`
	Address string `json:"address"`
}

// Pool is a deployed Uniswap V3 pool.
type Pool struct {
	Address string `json:"address"`
	Token0  string `json:"token0"`
	Token1  string `json:"token1"`
	Fee     int    `json:"fee"`
}

// Deployment is the full output of the TS deployer.
type Deployment struct {
	// Factory is the UniswapV3Factory address.
	Factory string `json:"factory"`
	// Callee is the TestUniswapV3Callee router used to drive swaps/mints.
	Callee string `json:"callee"`
	// GasToken is the ERC20 gas token (USDT0) address, if known. Optional.
	GasToken string `json:"gasToken"`
	// Destructible is a contract exposing create/selfdestruct edge-case methods.
	Destructible string  `json:"destructible"`
	Tokens       []Token `json:"tokens"`
	Pools        []Pool  `json:"pools"`
	// Contracts is a generic name->address registry for arbitrary (non-preset)
	// scenarios. Referenced from config via "@name".
	Contracts map[string]string `json:"contracts"`
}

// Registry returns a name->address map combining the preset fields and the
// generic Contracts map. Used to resolve "@name" references in config.
func (d *Deployment) Registry() map[string]string {
	m := map[string]string{}
	if d.Factory != "" {
		m["factory"] = d.Factory
	}
	if d.Callee != "" {
		m["callee"] = d.Callee
	}
	if d.Destructible != "" {
		m["destructible"] = d.Destructible
	}
	if d.GasToken != "" {
		m["gasToken"] = d.GasToken
	}
	for i, t := range d.Tokens {
		if t.Symbol != "" {
			m[t.Symbol] = t.Address
		}
		m[fmt.Sprintf("token%d", i)] = t.Address
	}
	for i, p := range d.Pools {
		m[fmt.Sprintf("pool%d", i)] = p.Address
	}
	for k, v := range d.Contracts {
		m[k] = v
	}
	return m
}

// ResolveAddr resolves "@name" against the registry, or returns s unchanged for
// a literal 0x address.
func (d *Deployment) ResolveAddr(s string) (string, bool) {
	if len(s) > 1 && s[0] == '@' {
		v, ok := d.Registry()[s[1:]]
		return v, ok
	}
	return s, true
}

// Load reads deployment.json.
func Load(path string) (*Deployment, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read deployment %s: %w", path, err)
	}
	var d Deployment
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("parse deployment %s: %w", path, err)
	}
	return &d, nil
}

// FactoryAddr / CalleeAddr / etc. return parsed addresses.
func (d *Deployment) FactoryAddr() common.Address      { return common.HexToAddress(d.Factory) }
func (d *Deployment) CalleeAddr() common.Address       { return common.HexToAddress(d.Callee) }
func (d *Deployment) DestructibleAddr() common.Address { return common.HexToAddress(d.Destructible) }

// TokenAddr returns the address of the token with the given symbol.
func (d *Deployment) TokenAddr(symbol string) (common.Address, bool) {
	for _, t := range d.Tokens {
		if t.Symbol == symbol {
			return common.HexToAddress(t.Address), true
		}
	}
	return common.Address{}, false
}

// FirstPool returns the first deployed pool, if any.
func (d *Deployment) FirstPool() (Pool, bool) {
	if len(d.Pools) == 0 {
		return Pool{}, false
	}
	return d.Pools[0], true
}
