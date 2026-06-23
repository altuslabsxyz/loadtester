// Package harness orchestrates the load-tester phases:
// setup -> observe(start) -> load -> drain -> report.
package harness

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/stablelabs/loadtester/accounts"
	"github.com/stablelabs/loadtester/collector"
	"github.com/stablelabs/loadtester/config"
	"github.com/stablelabs/loadtester/deployment"
	"github.com/stablelabs/loadtester/report"
	"github.com/stablelabs/loadtester/workload"
)

// poolSink is the shared sink for sent txs.
var poolSink workload.Sink

// Run executes the whole flow and writes a report into outDir.
func Run(ctx context.Context, targetPath, deploymentPath, outDir string) error {
	tgt, err := config.Load(targetPath)
	if err != nil {
		return err
	}
	dep, err := deployment.Load(deploymentPath)
	if err != nil {
		return err
	}
	log.Printf("[setup] target=%s chainId=%d nodes=%d", tgt.Name, tgt.ChainID, len(tgt.Nodes))

	abis, err := workload.LoadABIs()
	if err != nil {
		return err
	}

	// --- setup: connect pool, fund accounts, prepare token balances ---
	pool, err := accounts.NewPool(ctx, tgt.PrimaryJSONRPC(), tgt.Funding.MasterKey, tgt.Funding.AccountsN, tgt.ChainID)
	if err != nil {
		return fmt.Errorf("setup pool: %w", err)
	}
	log.Printf("[setup] chainId verified, %d accounts generated", len(pool.Accs))
	if err := pool.Fund(ctx, tgt.Funding.FundPerAccount); err != nil {
		return fmt.Errorf("fund accounts: %w", err)
	}
	log.Printf("[setup] accounts funded")

	builder := workload.NewBuilder(pool, abis, dep)
	if err := builder.PrepareAccounts(ctx); err != nil {
		return fmt.Errorf("prepare token balances: %w", err)
	}
	log.Printf("[setup] token balances/approvals prepared")

	// --- observe: start collectors ---
	primaryComet := firstCometRPC(tgt)
	var memCol *collector.MempoolCollector
	if primaryComet != "" {
		memCol, err = collector.NewMempoolCollector(ctx, primaryComet, tgt.PrimaryJSONRPC())
		if err != nil {
			return fmt.Errorf("mempool collector: %w", err)
		}
	}
	appCol := collector.NewAppHashCollector(cometNodes(tgt))

	obsCtx, stopObs := context.WithCancel(ctx)
	defer stopObs()
	poll := time.Duration(tgt.Observe.PollIntervalMs) * time.Millisecond
	if memCol != nil {
		go memCol.Run(obsCtx, poll)
	}
	go appCol.Run(obsCtx, poll)
	log.Printf("[observe] collectors started")

	buildInput := func() report.Input {
		in := report.Input{
			TargetName: tgt.Name,
			ChainID:    tgt.ChainID,
			Continuous: tgt.Workload.Continuous(),
			AppHash:    appCol.Result(),
			SentTotal:  poolSink.Total(),
			Sent:       poolSink.Stats(),
		}
		if memCol != nil {
			in.Mempool = memCol.Result()
		}
		return in
	}
	writeReport := func(tag string) {
		if mdPath, werr := report.Write(outDir, buildInput()); werr != nil {
			log.Printf("[report] write error: %v", werr)
		} else {
			log.Printf("[report] %s written to %s", tag, mdPath)
		}
	}

	// --- load ---
	driver, err := workload.NewDriver(ctx, pool, builder, &poolSink)
	if err != nil {
		return fmt.Errorf("driver: %w", err)
	}
	specs := buildSpecs(tgt, builder)

	if tgt.Workload.Continuous() {
		reportEvery := time.Duration(tgt.Observe.ReportIntervalSec) * time.Second
		if reportEvery <= 0 {
			reportEvery = 30 * time.Second
		}
		log.Printf("[load] CONTINUOUS: %d workload(s) until interrupted (Ctrl+C); report every %s", len(specs), reportEvery)

		var rwg sync.WaitGroup
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			t := time.NewTicker(reportEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					writeReport(fmt.Sprintf("snapshot (sent=%d)", poolSink.Total()))
				}
			}
		}()
		driver.Run(ctx, 0, specs)
		rwg.Wait()
		stopObs()
		time.Sleep(500 * time.Millisecond)
		writeReport("final")
		log.Printf("[done] continuous run stopped; %d txs sent", poolSink.Total())
		return nil
	}

	// One-shot.
	dur := time.Duration(tgt.Workload.DurationSec) * time.Second
	log.Printf("[load] running %d workload(s) for %s", len(specs), dur)
	driver.Run(ctx, dur, specs)
	log.Printf("[load] done; %d txs sent", poolSink.Total())

	// Adaptive drain: poll until the mempool empties, capped.
	if primaryComet != "" {
		drainCap := time.Duration(tgt.Observe.DrainWindowSec) * time.Second
		if drainCap < 5*time.Minute {
			drainCap = 5 * time.Minute
		}
		log.Printf("[drain] polling mempool until empty (cap %s)", drainCap)
		deadline := time.Now().Add(drainCap)
		zeros := 0
		for time.Now().Before(deadline) {
			n, derr := collector.NumUnconfirmedTxs(ctx, primaryComet)
			if derr == nil {
				if n == 0 {
					if zeros++; zeros >= 2 {
						log.Printf("[drain] mempool drained to 0")
						break
					}
				} else {
					zeros = 0
				}
				log.Printf("[drain] CList=%d", n)
			}
			if !sleepCtx(ctx, 3*time.Second) {
				log.Printf("[drain] interrupted")
				break
			}
		}
	} else {
		sleepCtx(ctx, time.Duration(tgt.Observe.DrainWindowSec)*time.Second)
	}

	stopObs()
	time.Sleep(500 * time.Millisecond)
	writeReport("final")
	return nil
}

func buildSpecs(tgt *config.Target, builder *workload.Builder) []workload.Spec {
	var specs []workload.Spec
	for key, load := range tgt.Workload.Lanes {
		kind := workload.Kind(key)
		if load.Enabled != nil && !*load.Enabled {
			continue
		}
		if (kind == workload.KindSelfDestruct || kind == workload.KindBump) && !tgt.Workload.AllowDestructive {
			log.Printf("[load] skipping %s (allowDestructive=false)", kind)
			continue
		}
		if !builder.Supports(kind) {
			log.Printf("[load] skipping %s (deployment lacks required contracts)", kind)
			continue
		}
		specs = append(specs, workload.Spec{Kind: kind, Inflight: load.TargetInflight})
	}
	return specs
}

func firstCometRPC(tgt *config.Target) string {
	for _, n := range tgt.Nodes {
		if n.CometRPC != "" {
			return n.CometRPC
		}
	}
	return ""
}

func cometNodes(tgt *config.Target) []collector.NodeRPC {
	var out []collector.NodeRPC
	for _, n := range tgt.CometRPCs() {
		out = append(out, collector.NodeRPC{Name: n.Name, CometRPC: n.CometRPC})
	}
	return out
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
