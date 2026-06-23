// Package harness orchestrates the load-tester phases:
// setup -> lanes -> observe(start) -> load -> drain -> report.
package harness

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	sdk "github.com/cosmos/cosmos-sdk/types"

	stableapp "github.com/stablelabs/stable/app"
	appconfig "github.com/stablelabs/stable/app/config"

	"github.com/stablelabs/loadtester/accounts"
	"github.com/stablelabs/loadtester/collector"
	"github.com/stablelabs/loadtester/config"
	"github.com/stablelabs/loadtester/deployment"
	"github.com/stablelabs/loadtester/gov"
	"github.com/stablelabs/loadtester/laneplan"
	"github.com/stablelabs/loadtester/report"
	"github.com/stablelabs/loadtester/workload"
)

var prefixOnce bool

// initSDKConfig sets the global bech32 prefixes to "stable" so gov authority
// addresses serialize correctly. Safe to call once before the config is sealed.
func initSDKConfig() {
	if prefixOnce {
		return
	}
	cfg := sdk.GetConfig()
	appconfig.SetBech32Prefixes(cfg)
	prefixOnce = true
}

// Run executes the whole flow and writes a report into outDir. failOn is the
// CI exit-code threshold ("none"|"fail"|"review") applied to the final one-shot
// verdict; continuous runs (verdict LIVE) never trip it.
func Run(ctx context.Context, targetPath, deploymentPath, outDir, failOn string) error {
	initSDKConfig()
	runStart := time.Now() // log-scan ignores files older than this (stale prior-run logs)

	tgt, err := config.Load(targetPath)
	if err != nil {
		return err
	}
	dep, err := deployment.Load(deploymentPath)
	if err != nil {
		return err
	}
	log.Printf("[setup] target=%s chainId=%d nodes=%d gov=%s", tgt.Name, tgt.ChainID, len(tgt.Nodes), tgt.Governance.Mode)

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

	var plan *laneplan.Plan
	if len(tgt.Blockspace.Lanes) > 0 {
		plan, err = laneplan.BuildFromConfig(tgt.Blockspace, dep)
		if err != nil {
			return fmt.Errorf("build lane plan from config: %w", err)
		}
		log.Printf("[setup] lane plan from config: %d lane(s), weight=%d%%", len(tgt.Blockspace.Lanes), plan.MaxBlockspaceGasWeight)
	} else {
		plan = laneplan.Build(dep, abis)
		log.Printf("[setup] lane plan: built-in preset")
	}
	builder := workload.NewBuilder(pool, abis, dep, plan.ExpectedLane)
	if err := builder.PrepareAccounts(ctx); err != nil {
		return fmt.Errorf("prepare token balances: %w", err)
	}
	log.Printf("[setup] token balances/approvals prepared")

	// --- lanes: register via governance (or read preconfigured) ---
	govNode := tgt.Nodes[clamp(tgt.Governance.VotingNode, 0, len(tgt.Nodes)-1)]
	encCfg := stableapp.MakeEncodingConfig(tgt.ChainID)
	registrar, err := gov.New(ctx, govNode, tgt.ChainID, encCfg.Codec)
	if err != nil {
		return fmt.Errorf("gov registrar: %w", err)
	}
	effParams, err := registrar.Register(ctx, tgt.Governance.Mode, plan, tgt.Governance)
	if err != nil {
		return fmt.Errorf("register lanes: %w", err)
	}
	// On a JSON-RPC-only target (no cosmos gRPC) the lane params cannot be read
	// from chain; preconfigured mode then assumes the config-declared plan. Flag
	// that in the report so quota attribution isn't mistaken for verified state.
	paramsVerified := govNode.GRPC != ""
	if !paramsVerified {
		log.Printf("[lanes] WARNING: no gRPC - lane params ASSUMED from config, not verified on-chain")
	}
	log.Printf("[lanes] effective params: %d vip, %d tx-type lanes, weight=%d%%",
		len(effParams.VipLanes), len(effParams.TxTypeLanes), effParams.MaxBlockspaceGasWeight)

	// VIP nonce-key bit must match a lane id actually registered ON-CHAIN. Prefer
	// the declared/preset VIP id if it exists on-chain; otherwise fall back to the
	// first on-chain VIP lane (with a warning). The tool can only drive ONE VIP
	// lane, so note when several exist.
	if len(effParams.VipLanes) > 0 {
		want := plan.ExpectedLane[workload.KindVIP]
		chosen, found := effParams.VipLanes[0].Id, false
		for _, vl := range effParams.VipLanes {
			if vl.Id == want {
				chosen, found = want, true
				break
			}
		}
		if !found {
			log.Printf("[lanes] WARNING: declared VIP lane id %d not on-chain; using on-chain VIP lane %d", want, chosen)
		}
		if len(effParams.VipLanes) > 1 {
			log.Printf("[lanes] NOTE: %d VIP lanes on-chain; only lane %d will receive VIP traffic", len(effParams.VipLanes), chosen)
		}
		builder.SetVIPLane(chosen)
	}
	// Reconcile config-declared lanes against on-chain params (only meaningful
	// when params are verified, i.e. gRPC available). Surfaces a config that
	// declares lanes not actually registered - the operator's stated lanes are
	// what they expect to verify, so a mismatch must be loud.
	lanesReconciled := paramsVerified
	if paramsVerified && len(tgt.Blockspace.Lanes) > 0 {
		onchain := map[int32]bool{}
		for _, l := range effParams.VipLanes {
			onchain[l.Id] = true
		}
		for _, l := range effParams.TxTypeLanes {
			onchain[l.Id] = true
		}
		var missing []int32
		for _, l := range tgt.Blockspace.Lanes {
			if !onchain[l.ID] {
				missing = append(missing, l.ID)
			}
		}
		if len(missing) > 0 {
			lanesReconciled = false
			log.Printf("[lanes] WARNING: declared lanes NOT found on-chain: %v - "+
				"Goal 1 for those lanes is meaningless; fix the target YAML", missing)
		}
	}

	// --- observe: start collectors ---
	primaryComet := firstCometRPC(tgt)
	maxBlockGas := uint64(0)
	if primaryComet != "" {
		if g, err := collector.MaxBlockGas(ctx, primaryComet); err == nil {
			maxBlockGas = g
		} else {
			log.Printf("[observe] WARNING: could not read block.max_gas over CometRPC: %v", err)
		}
	}
	// JSON-RPC-only fallback: when CometBFT consensus_params is unreachable (a
	// public testnet typically exposes only EVM JSON-RPC), derive the quota
	// denominator from the latest block's gas limit instead of leaving Goal 1
	// permanently NOT EVALUATED.
	if maxBlockGas == 0 {
		if g, err := collector.BlockGasLimit(ctx, pool.Client); err == nil && g > 0 {
			maxBlockGas = g
			log.Printf("[observe] block.max_gas via JSON-RPC block gasLimit fallback: %d", maxBlockGas)
		}
	}
	if maxBlockGas == 0 {
		log.Printf("[observe] WARNING: block.max_gas is 0 (unknown/unlimited) - Goal 1 lane quotas are undefined and will NOT be evaluated")
	}
	classifier := collector.NewClassifier(effParams, ethtypes.LatestSignerForChainID(pool.ChainID))
	laneCol := collector.NewLaneCollector(pool.Client, classifier, effParams, maxBlockGas)

	// Derive each workload's expected lane by classifying a sample tx against the
	// effective on-chain params (decouples workload->lane from hardcoded ids).
	if feeCap, tip, ferr := pool.Fees(ctx); ferr == nil && len(pool.Accs) > 0 {
		builder.DeriveExpectedLanes(classifier.PrimaryLane, pool.Accs[0], feeCap, tip)
	}

	// The mempool collector tracks the CometBFT CList depth (num_unconfirmed_txs).
	// The EVM txpool RPCs are NOT used: on the stable chain they are vestigial and
	// always report 0. Without a CometRPC there is no reliable mempool signal, so
	// Goal 2 is reported NOT EVALUATED (never a false PASS).
	memCol := collector.NewMempoolCollector(primaryComet)
	appCol := collector.NewAppHashCollector(cometNodes(tgt), pool.Client)

	obsCtx, stopObs := context.WithCancel(ctx)
	defer stopObs() // safety: ensure collectors stop on any early return
	poll := time.Duration(tgt.Observe.PollIntervalMs) * time.Millisecond
	go laneCol.Run(obsCtx, poll)
	go memCol.Run(obsCtx, poll)
	go appCol.Run(obsCtx, poll)
	log.Printf("[observe] collectors started (maxBlockGas=%d)", maxBlockGas)

	// report builder/writer closures (shared by one-shot and continuous modes).
	buildInput := func() report.Input {
		in := report.Input{
			TargetName:      tgt.Name,
			ChainID:         tgt.ChainID,
			GovMode:         string(tgt.Governance.Mode),
			MaxBlockGas:     maxBlockGas,
			Continuous:      tgt.Workload.Continuous(),
			ParamsVerified:  paramsVerified,
			LanesReconciled: lanesReconciled,
			Lane:            laneCol.Result(),
			AppHash:         appCol.Result(),
			LogScan:         collector.NewLogScanCollector(tgt.LogPaths, runStart).Scan(),
			SentTotal:       poolSink.Total(),
			Sent:            poolSink.Stats(),
			Mempool:         memCol.Result(),
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

	// --- load: drive workloads ---
	// VIP (2D-nonce) txs are only accepted by the role:vip node, so they go to a
	// dedicated client. Without a role:vip node the VIP workload is skipped.
	var vipClient *ethclient.Client
	if vrpc := tgt.VIPJSONRPC(); vrpc != "" {
		c, _, verr := accounts.Connect(ctx, vrpc, tgt.ChainID)
		if verr != nil {
			return fmt.Errorf("vip rpc %s: %w", vrpc, verr)
		}
		vipClient = c
		log.Printf("[load] VIP endpoint: %s", vrpc)
	} else {
		log.Printf("[load] no VIP RPC (role:vip node) configured - VIP txs will be skipped")
	}
	driver, err := workload.NewDriver(ctx, pool, builder, &poolSink, vipClient)
	if err != nil {
		return fmt.Errorf("driver: %w", err)
	}
	specs := buildSpecs(tgt, builder)

	if tgt.Workload.Continuous() {
		reportEvery := time.Duration(tgt.Observe.ReportIntervalSec) * time.Second
		if reportEvery <= 0 {
			reportEvery = 30 * time.Second
		}
		log.Printf("[load] CONTINUOUS: %d workload(s) running until interrupted (Ctrl+C); report every %s",
			len(specs), reportEvery)

		// Periodic snapshot reporter.
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

		driver.Run(ctx, 0, specs) // blocks until ctx cancelled (SIGINT)
		rwg.Wait()
		stopObs()
		time.Sleep(500 * time.Millisecond)
		writeReport("final")
		log.Printf("[done] continuous run stopped; %d txs sent", poolSink.Total())
		return nil
	}

	// One-shot mode.
	dur := time.Duration(tgt.Workload.DurationSec) * time.Second
	log.Printf("[load] running %d workload(s) for %s", len(specs), dur)
	driver.Run(ctx, dur, specs)
	log.Printf("[load] done; %d txs sent", poolSink.Total())

	// --- drain: poll until the mempool actually empties (adaptive), capped ---
	if primaryComet != "" {
		drainCap := time.Duration(tgt.Observe.DrainWindowSec) * time.Second
		if drainCap < 5*time.Minute {
			drainCap = 5 * time.Minute
		}
		log.Printf("[drain] polling mempool until empty (cap %s)", drainCap)
		deadline := time.Now().Add(drainCap)
		zeros, prev, flat := 0, -1, 0
		for time.Now().Before(deadline) {
			n, derr := collector.NumUnconfirmedTxs(ctx, primaryComet)
			if derr == nil {
				if n == 0 {
					zeros++
					if zeros >= 2 {
						log.Printf("[drain] mempool drained to 0")
						break
					}
				} else {
					zeros = 0
				}
				// Stop early on a flat residual tail: if depth hasn't decreased for
				// ~30s it's a small backlog the proposer isn't pulling (TxProvider),
				// not draining further - no point burning the full cap.
				if n == prev {
					if flat++; flat >= 20 && n > 0 {
						log.Printf("[drain] CList flat at %d for ~60s - residual tail, stopping", n)
						break
					}
				} else {
					flat = 0
				}
				prev = n
				log.Printf("[drain] CList=%d", n)
			}
			if !sleepCtx(ctx, 3*time.Second) {
				log.Printf("[drain] interrupted")
				break
			}
		}
	} else {
		// No CometRPC: the CList depth (the only trustworthy mempool signal on the
		// stable chain) is unobservable here, and the EVM txpool RPCs are vestigial
		// (always 0). There is nothing reliable to poll, so just wait the drain
		// window; Goal 2 is reported NOT EVALUATED.
		drain := time.Duration(tgt.Observe.DrainWindowSec) * time.Second
		log.Printf("[drain] no CometRPC; mempool drain not observable (waiting %s, Goal 2 = NOT EVALUATED)", drain)
		sleepCtx(ctx, drain)
	}

	stopObs()
	time.Sleep(500 * time.Millisecond) // let collectors flush final samples
	writeReport("final")

	// CI gate: exit non-zero when the overall verdict meets the --fail-on
	// threshold. Continuous mode returns earlier (Goal 2 is LIVE), so this only
	// applies to one-shot runs.
	verdicts := report.Evaluate(buildInput())
	log.Printf("[done] overall verdict: %s (goal1=%s goal2=%s goal3=%s)",
		verdicts.Overall, verdicts.Goal1, verdicts.Goal2, verdicts.Goal3)
	if verdicts.Fails(failOn) {
		return fmt.Errorf("verdict %s meets --fail-on=%s", verdicts.Overall, failOn)
	}
	return nil
}

// poolSink is the shared sink for sent txs.
var poolSink workload.Sink

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

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
