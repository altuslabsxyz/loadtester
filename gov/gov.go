package gov

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/cosmos/cosmos-sdk/codec"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types"

	"github.com/stablelabs/loadtester/config"
	"github.com/stablelabs/loadtester/laneplan"
	stabletypes "github.com/stablelabs/stable/x/stable/types"
)

// govPrecompile is the cosmos-evm gov precompile address (see txtrigger scripts).
var govPrecompile = common.HexToAddress("0x0000000000000000000000000000000000000805")

const govABIJSON = `[
  {"name":"submitProposal","type":"function","inputs":[
    {"name":"proposer","type":"address"},
    {"name":"jsonProposal","type":"bytes"},
    {"name":"deposit","type":"tuple[]","components":[{"name":"denom","type":"string"},{"name":"amount","type":"uint256"}]}
  ],"outputs":[{"name":"proposalId","type":"uint64"}]},
  {"name":"vote","type":"function","inputs":[
    {"name":"voter","type":"address"},
    {"name":"proposalId","type":"uint64"},
    {"name":"option","type":"uint8"},
    {"name":"metadata","type":"string"}
  ],"outputs":[{"name":"success","type":"bool"}]}
]`

type depositTuple struct {
	Denom  string
	Amount *big.Int
}

// Registrar submits + votes lane-registration proposals via the gov precompile.
type Registrar struct {
	client  *ethclient.Client
	chainID *big.Int
	signer  types.Signer
	cdc     codec.Codec
	govABI  abi.ABI
	grpc    string
}

// New builds a Registrar bound to a node's JSON-RPC + gRPC endpoints.
func New(ctx context.Context, node config.Node, chainID uint64, cdc codec.Codec) (*Registrar, error) {
	c, err := ethclient.DialContext(ctx, node.JSONRPC)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", node.JSONRPC, err)
	}
	id, err := c.ChainID(ctx)
	if err != nil {
		return nil, err
	}
	if id.Uint64() != chainID {
		return nil, fmt.Errorf("chain id mismatch on %s: want %d got %d", node.JSONRPC, chainID, id.Uint64())
	}
	parsed, err := abi.JSON(strings.NewReader(govABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse gov abi: %w", err)
	}
	return &Registrar{
		client:  c,
		chainID: id,
		signer:  types.LatestSignerForChainID(id),
		cdc:     cdc,
		govABI:  parsed,
		grpc:    node.GRPC,
	}, nil
}

// Register applies the lane plan according to the governance mode and returns
// the effective on-chain params the collector should classify against.
func (r *Registrar) Register(ctx context.Context, mode config.GovMode, plan *laneplan.Plan, gov config.Governance) (*stabletypes.Params, error) {
	if mode == config.GovPreconfigured {
		return QueryParams(ctx, r.grpc)
	}

	// Idempotent: if the plan's lanes are already registered, skip submission and
	// reuse them. Lets continuous runs reattach without re-proposing.
	if r.grpc != "" {
		if p, err := QueryParams(ctx, r.grpc); err == nil {
			wantVip := make([]int32, 0, len(plan.VipLanes))
			for _, l := range plan.VipLanes {
				wantVip = append(wantVip, l.Id)
			}
			wantTx := make([]int32, 0, len(plan.TxTypeLanes))
			for _, l := range plan.TxTypeLanes {
				wantTx = append(wantTx, l.Id)
			}
			if hasLanes(p, wantVip, wantTx) {
				return p, nil
			}
		}
	}

	proposalID, err := r.submitProposal(ctx, plan, gov)
	if err != nil {
		return nil, fmt.Errorf("submit proposal: %w", err)
	}
	for i, vk := range gov.VoterKeys {
		if err := r.vote(ctx, vk, proposalID); err != nil {
			return nil, fmt.Errorf("vote[%d]: %w", i, err)
		}
	}

	timeout := 2 * time.Minute
	if mode == config.GovRealVote {
		timeout = 20 * time.Minute
	}
	if err := r.waitForLanes(ctx, plan, timeout); err != nil {
		return nil, err
	}
	return QueryParams(ctx, r.grpc)
}

// govAuthority is the bech32 address of the gov module account (proposal signer).
func govAuthority() string {
	return authtypes.NewModuleAddress(govtypes.ModuleName).String()
}

func (r *Registrar) submitProposal(ctx context.Context, plan *laneplan.Plan, gov config.Governance) (uint64, error) {
	msg := &stabletypes.MsgUpdateParams{
		Authority: govAuthority(),
		Params:    plan.Params(),
	}
	msgJSON, err := r.cdc.MarshalInterfaceJSON(msg)
	if err != nil {
		return 0, fmt.Errorf("marshal MsgUpdateParams: %w", err)
	}

	proposal := map[string]any{
		"messages":  []json.RawMessage{msgJSON},
		"metadata":  "",
		"title":     "load-tester: register lanes",
		"summary":   "Register Tx-Type/VIP lanes for QA load testing",
		"expedited": false,
	}
	proposalBytes, err := json.Marshal(proposal)
	if err != nil {
		return 0, err
	}

	key, proposer, err := parseKey(gov.ProposerKey)
	if err != nil {
		return 0, err
	}
	denom, amount := splitCoin(gov.Deposit)
	data, err := r.govABI.Pack("submitProposal", proposer, proposalBytes, []depositTuple{{Denom: denom, Amount: amount}})
	if err != nil {
		return 0, fmt.Errorf("pack submitProposal: %w", err)
	}
	receipt, err := r.sendSignedGovTx(ctx, key, proposer, data, 1_500_000)
	if err != nil {
		return 0, err
	}
	id, ok := extractProposalID(receipt)
	if !ok {
		return 0, fmt.Errorf("could not extract proposal id from receipt %s", receipt.TxHash.Hex())
	}
	return id, nil
}

func (r *Registrar) vote(ctx context.Context, voterKeyHex string, proposalID uint64) error {
	key, addr, err := parseKey(voterKeyHex)
	if err != nil {
		return err
	}
	// option 1 = YES
	data, err := r.govABI.Pack("vote", addr, proposalID, uint8(1), "")
	if err != nil {
		return fmt.Errorf("pack vote: %w", err)
	}
	_, err = r.sendSignedGovTx(ctx, key, addr, data, 500_000)
	return err
}

// sendSignedGovTx signs a legacy tx to the gov precompile and waits for the
// receipt. Gas price is taken from the node's suggestion so the tx clears the
// chain's minimum global fee (some chains reject gasPrice=0).
func (r *Registrar) sendSignedGovTx(ctx context.Context, key *ecdsa.PrivateKey, from common.Address, data []byte, gas uint64) (*types.Receipt, error) {
	nonce, err := r.client.PendingNonceAt(ctx, from)
	if err != nil {
		return nil, err
	}
	gasPrice, err := r.client.SuggestGasPrice(ctx)
	if err != nil || gasPrice == nil || gasPrice.Sign() == 0 {
		// Fallback: base fee from the latest header, or a small floor.
		gasPrice = big.NewInt(1_000_000_000) // 1 gwei
		if head, herr := r.client.HeaderByNumber(ctx, nil); herr == nil && head.BaseFee != nil && head.BaseFee.Sign() > 0 {
			gasPrice = new(big.Int).Mul(head.BaseFee, big.NewInt(2))
		}
	}
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: gasPrice,
		Gas:      gas,
		To:       &govPrecompile,
		Value:    big.NewInt(0),
		Data:     data,
	})
	signed, err := types.SignTx(tx, r.signer, key)
	if err != nil {
		return nil, err
	}
	if err := r.client.SendTransaction(ctx, signed); err != nil {
		return nil, fmt.Errorf("send gov tx: %w", err)
	}
	return r.waitReceipt(ctx, signed.Hash(), 60*time.Second)
}

func (r *Registrar) waitReceipt(ctx context.Context, hash common.Hash, timeout time.Duration) (*types.Receipt, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		rc, err := r.client.TransactionReceipt(ctx, hash)
		if err == nil && rc != nil {
			if rc.Status != 1 {
				return rc, fmt.Errorf("gov tx %s reverted", hash.Hex())
			}
			return rc, nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return nil, fmt.Errorf("gov tx %s not mined in %s", hash.Hex(), timeout)
}

func (r *Registrar) waitForLanes(ctx context.Context, plan *laneplan.Plan, timeout time.Duration) error {
	wantVip := make([]int32, 0, len(plan.VipLanes))
	for _, l := range plan.VipLanes {
		wantVip = append(wantVip, l.Id)
	}
	wantTx := make([]int32, 0, len(plan.TxTypeLanes))
	for _, l := range plan.TxTypeLanes {
		wantTx = append(wantTx, l.Id)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p, err := QueryParams(ctx, r.grpc)
		if err == nil && hasLanes(p, wantVip, wantTx) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("lanes did not take effect within %s (proposal may not have passed)", timeout)
}

// extractProposalID finds the SubmitProposal(address,uint64) event and reads the id.
func extractProposalID(receipt *types.Receipt) (uint64, bool) {
	topic := crypto.Keccak256Hash([]byte("SubmitProposal(address,uint64)"))
	for _, lg := range receipt.Logs {
		if lg.Address != govPrecompile || len(lg.Topics) == 0 || lg.Topics[0] != topic {
			continue
		}
		if len(lg.Data) >= 32 {
			// id is the last 32-byte word (whether or not proposer is indexed).
			id := new(big.Int).SetBytes(lg.Data[len(lg.Data)-32:])
			return id.Uint64(), true
		}
	}
	return 0, false
}

func parseKey(hexKey string) (*ecdsa.PrivateKey, common.Address, error) {
	k, err := crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(hexKey), "0x"))
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("parse key: %w", err)
	}
	return k, crypto.PubkeyToAddress(k.PublicKey), nil
}

// splitCoin splits "50000...astable" into ("astable", 50000...).
func splitCoin(s string) (string, *big.Int) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "astable", big.NewInt(0)
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	amount, _ := new(big.Int).SetString(s[:i], 10)
	if amount == nil {
		amount = big.NewInt(0)
	}
	denom := strings.TrimSpace(s[i:])
	if denom == "" {
		denom = "astable"
	}
	return denom, amount
}
