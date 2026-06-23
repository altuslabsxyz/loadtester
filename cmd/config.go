package cmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/spf13/cobra"

	"github.com/stablelabs/loadtester/config"
)

var cfgTarget string

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show the resolved settings for a target file",
	Long: "config loads a target YAML (applying defaults and validation) and prints\n" +
		"the effective settings, so you can confirm what `start` will use. Private\n" +
		"keys are masked; their derived addresses are shown.",
	RunE: func(_ *cobra.Command, _ []string) error {
		tgt, err := config.Load(cfgTarget)
		if err != nil {
			return err
		}
		fmt.Print(renderConfig(cfgTarget, tgt))
		return nil
	},
}

func init() {
	configCmd.Flags().StringVarP(&cfgTarget, "target", "t", defaultTargetPath(), "target environment YAML (scaffold one with `make config`)")
	rootCmd.AddCommand(configCmd)
}

func renderConfig(path string, t *config.Target) string {
	var b strings.Builder
	fmt.Fprintf(&b, "loadtester config — %s\n\n", path)

	fmt.Fprintf(&b, "Chain\n")
	fmt.Fprintf(&b, "  name:           %s\n", t.Name)
	fmt.Fprintf(&b, "  evm chainId:    %d\n", t.ChainID)
	fmt.Fprintf(&b, "  cosmos chainId: %s\n\n", orDash(t.CosmosChainID))

	fmt.Fprintf(&b, "Nodes (%d)\n", len(t.Nodes))
	nComet, nGRPC := 0, 0
	for _, n := range t.Nodes {
		fmt.Fprintf(&b, "  - %-10s [%s]  jsonrpc=%s  cometRPC=%s  grpc=%s\n",
			n.Name, orDash(string(n.Role)), orDash(n.JSONRPC), yesNo(n.CometRPC != ""), yesNo(n.GRPC != ""))
		if n.CometRPC != "" {
			nComet++
		}
		if n.GRPC != "" {
			nGRPC++
		}
	}
	fmt.Fprintf(&b, "  load endpoint:  %s\n", t.PrimaryJSONRPC())
	if v := t.VIPJSONRPC(); v != "" {
		fmt.Fprintf(&b, "  vip endpoint:   %s (role:vip node)\n", v)
	} else {
		fmt.Fprintf(&b, "  vip endpoint:   none (no role:vip node) - VIP txs will be SKIPPED\n")
	}
	fmt.Fprintf(&b, "  CometRPC nodes: %d   gRPC nodes: %d\n\n", nComet, nGRPC)

	fmt.Fprintf(&b, "Funding\n")
	fmt.Fprintf(&b, "  masterKey:      %s\n", keyDesc(t.Funding.MasterKey))
	fmt.Fprintf(&b, "  accountsN:      %d\n", t.Funding.AccountsN)
	fmt.Fprintf(&b, "  fundPerAccount: %s\n\n", orDash(t.Funding.FundPerAccount))

	fmt.Fprintf(&b, "Governance\n")
	fmt.Fprintf(&b, "  mode:        %s\n", t.Governance.Mode)
	fmt.Fprintf(&b, "  proposer:    %s\n", keyDesc(t.Governance.ProposerKey))
	fmt.Fprintf(&b, "  voterKeys:   %d\n", len(t.Governance.VoterKeys))
	fmt.Fprintf(&b, "  deposit:     %s\n", orDash(t.Governance.Deposit))
	fmt.Fprintf(&b, "  votingNode:  %d\n\n", t.Governance.VotingNode)

	fmt.Fprintf(&b, "Lanes\n")
	if len(t.Blockspace.Lanes) > 0 {
		fmt.Fprintf(&b, "  source: config blockspace (%d lane(s)), maxBlockspaceGasWeight=%d%%\n",
			len(t.Blockspace.Lanes), t.Blockspace.MaxBlockspaceGasWeight)
		for _, l := range t.Blockspace.Lanes {
			if l.VIP {
				fmt.Fprintf(&b, "  - id=%-3d %-8s weight=%d  vip\n", l.ID, l.Name, l.Weight)
				continue
			}
			fmt.Fprintf(&b, "  - id=%-3d %-8s weight=%d  to=%v methods=%v txTypes=%v%s\n",
				l.ID, l.Name, l.Weight, l.ToAddrs, l.Methods, l.TxTypes, noOverflow(l.NoOverflow))
		}
	} else {
		fmt.Fprintf(&b, "  source: built-in preset (erc20 + swap + vip), resolved at runtime from deployment.json\n")
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "Workload\n")
	if t.Workload.Continuous() {
		fmt.Fprintf(&b, "  mode:        continuous (durationSec=%d, runs until Ctrl+C)\n", t.Workload.DurationSec)
	} else {
		fmt.Fprintf(&b, "  mode:        one-shot (durationSec=%d)\n", t.Workload.DurationSec)
	}
	fmt.Fprintf(&b, "  destructive: %s\n", allowedBlocked(t.Workload.AllowDestructive))
	fmt.Fprintf(&b, "  lanes:\n")
	for _, k := range sortedKeys(t.Workload.Lanes) {
		l := t.Workload.Lanes[k]
		state := "enabled"
		if l.Enabled != nil && !*l.Enabled {
			state = "disabled"
		}
		note := ""
		if (k == "bump" || k == "selfdestruct") && !t.Workload.AllowDestructive {
			note = "  (skipped: allowDestructive=false)"
		}
		fmt.Fprintf(&b, "    %-14s inflight=%-4d %s%s\n", k, l.TargetInflight, state, note)
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "Observe\n")
	fmt.Fprintf(&b, "  pollIntervalMs:    %d\n", t.Observe.PollIntervalMs)
	fmt.Fprintf(&b, "  drainWindowSec:    %d\n", t.Observe.DrainWindowSec)
	fmt.Fprintf(&b, "  stuckAfterBlocks:  %d\n", t.Observe.StuckAfterBlocks)
	fmt.Fprintf(&b, "  reportIntervalSec: %d\n\n", t.Observe.ReportIntervalSec)

	fmt.Fprintf(&b, "Logs\n")
	if len(t.LogPaths) > 0 {
		fmt.Fprintf(&b, "  logPaths: %d file(s) (local ground-truth for Goal 1/3)\n", len(t.LogPaths))
		for _, p := range t.LogPaths {
			fmt.Fprintf(&b, "    - %s\n", p)
		}
	} else {
		fmt.Fprintf(&b, "  logPaths: none (testnet: Goal 1 enforcement / Goal 3 log signals unavailable)\n")
	}

	return b.String()
}

func sortedKeys(m map[string]config.LaneLoad) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// keyDesc masks a private key and appends its derived address (which is public),
// so the operator can confirm WHICH account is configured without leaking the key.
func keyDesc(hexKey string) string {
	hexKey = strings.TrimSpace(hexKey)
	if hexKey == "" {
		return "(unset)"
	}
	masked := maskKey(hexKey)
	k, err := crypto.HexToECDSA(strings.TrimPrefix(hexKey, "0x"))
	if err != nil {
		return masked + " (unparseable)"
	}
	return fmt.Sprintf("%s  addr=%s", masked, crypto.PubkeyToAddress(k.PublicKey).Hex())
}

func maskKey(s string) string {
	s = strings.TrimPrefix(strings.TrimSpace(s), "0x")
	if len(s) <= 10 {
		return "0x****"
	}
	return "0x" + s[:6] + "…" + s[len(s)-4:]
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func allowedBlocked(b bool) string {
	if b {
		return "allowed"
	}
	return "blocked (allowDestructive=false)"
}

func noOverflow(b bool) string {
	if b {
		return " noOverflow"
	}
	return ""
}
