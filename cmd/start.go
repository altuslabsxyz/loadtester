package cmd

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/stablelabs/loadtester/harness"
)

var (
	flagTarget     string
	flagDeployment string
	flagOut        string
	flagFailOn     string
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Run the load test against the target chain",
	Long: "start funds accounts, registers lanes (per the target's governance mode),\n" +
		"drives the workload mix, and writes a report. With workload.durationSec <= 0\n" +
		"it runs continuously until Ctrl+C, writing periodic report snapshots.",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		return harness.Run(ctx, flagTarget, flagDeployment, flagOut, flagFailOn)
	},
}

func init() {
	startCmd.Flags().StringVarP(&flagTarget, "target", "t", defaultTargetPath(), "target environment YAML (scaffold one with `make config`)")
	startCmd.Flags().StringVarP(&flagDeployment, "deployment", "d", "deployment.json", "deployment JSON (from the TS deployer)")
	startCmd.Flags().StringVarP(&flagOut, "out", "o", "out", "report output directory")
	startCmd.Flags().StringVar(&flagFailOn, "fail-on", "none", "exit non-zero when the overall verdict meets this threshold: none|fail|review (one-shot only)")
	rootCmd.AddCommand(startCmd)
}
