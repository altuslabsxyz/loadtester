// Package cmd wires the loadtester CLI (cobra).
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "loadtester",
	Short: "Stable chain QA load-tester",
	Long: "loadtester drives EVM load against a Stable chain and verifies Guaranteed\n" +
		"Blockspace lane quotas, Selective-Recheck mempool drain, and BlockSTM/MemIAVL\n" +
		"determinism. Environment is a target.yaml; contracts come from deployment.json.",
}

// Execute runs the root command.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
