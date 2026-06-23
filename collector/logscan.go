package collector

import (
	"bufio"
	"os"
	"strings"
)

// LogScanResult is ground-truth scraped from node logs. Unlike post-commit RPC
// state, these lines are emitted by the proposer/consensus at the exact moment
// the behavior happens, so they can PROVE things RPC cannot:
//   - lane quota enforcement (the proposer logs when it skips a tx for quota)
//   - app-hash mismatch / consensus halt (the real non-determinism signal)
type LogScanResult struct {
	Available bool
	Files     []string

	// LaneQuotaSkips counts the PrepareProposal skip ("skip tx: lane gas quota
	// exceeded", selector.go logLaneGasSkip WARN). Each is the proposer actively
	// capping a lane - direct Goal-1 enforcement proof.
	LaneQuotaSkips   int
	LaneQuotaSamples []string

	// LaneQuotaRejects counts the ProcessProposal reject ("...process proposal
	// ... lane gas quota exceeded", handler.go). This is NOT healthy enforcement
	// - it means a proposer BUILT a quota-violating block that validators
	// rejected (a proposer/validator disagreement worth investigating).
	LaneQuotaRejects int

	// AppHashIssues holds lines indicating an app-hash mismatch / consensus
	// failure / panic - the genuine manifestation of non-determinism (a
	// divergent node halts; it cannot silently commit a different hash).
	AppHashIssues []string

	// Truncated is set if a log line exceeded the scan buffer (scan may be
	// incomplete - do not treat "no issues found" as authoritative).
	Truncated bool
}

// scan patterns. Verified against the chain/cometbft source (exact strings).
var (
	laneSkipNeedle    = "skip tx: lane gas quota exceeded" // selector.go WARN (enforcement)
	laneRejectNeedle1 = "process proposal"                 // handler.go reject context
	laneRejectNeedle2 = "lane gas quota exceeded"          // combined with the above
	appHashNeedles    = []string{
		"wrong Block.Header.AppHash", // cometbft state/validation.go (+ live panic wrap)
		"AppHash does not match",     // cometbft replay.go
		"CONSENSUS FAILURE",          // cometbft state.go (uppercase!)
	}
	panicNeedle = "panic:"
)

// LogScanCollector scrapes node log files once (at report time).
type LogScanCollector struct {
	paths []string
}

func NewLogScanCollector(paths []string) *LogScanCollector {
	return &LogScanCollector{paths: paths}
}

// Scan reads the configured log files and extracts ground-truth signals.
func (lc *LogScanCollector) Scan() LogScanResult {
	res := LogScanResult{}
	if len(lc.paths) == 0 {
		return res
	}
	for _, p := range lc.paths {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		res.Available = true
		res.Files = append(res.Files, p)
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024) // large lines (tx dumps)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.Contains(line, laneSkipNeedle):
				res.LaneQuotaSkips++
				if len(res.LaneQuotaSamples) < 3 {
					res.LaneQuotaSamples = append(res.LaneQuotaSamples, trim(line))
				}
				continue
			case strings.Contains(line, laneRejectNeedle1) && strings.Contains(line, laneRejectNeedle2):
				res.LaneQuotaRejects++
				continue
			}
			matched := false
			for _, n := range appHashNeedles {
				if strings.Contains(line, n) {
					matched = true
					break
				}
			}
			if !matched && strings.Contains(line, panicNeedle) && containsAny(line, "AppHash", "app_hash", "apphash") {
				matched = true
			}
			if matched && len(res.AppHashIssues) < 10 {
				res.AppHashIssues = append(res.AppHashIssues, trim(line))
			}
		}
		// A line longer than the buffer ends the scan early; surface that so a
		// truncated scan is not mistaken for "clean".
		if err := sc.Err(); err != nil {
			res.Truncated = true
		}
		f.Close()
	}
	return res
}

func trim(s string) string {
	if len(s) > 240 {
		return s[:240] + "..."
	}
	return s
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
