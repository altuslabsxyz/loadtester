package config

import (
	"os"
	"strings"
	"testing"
)

const validMasterKey = "1111111111111111111111111111111111111111111111111111111111111111"

func writeTarget(t *testing.T, body string) string {
	t.Helper()
	path := t.TempDir() + "/target.yaml"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func validTargetYAML(fundingExtra, workloadExtra string) string {
	return `
name: test
chainId: 999
nodes:
  - name: n0
    role: fullnode
    jsonrpc: http://127.0.0.1:8545
funding:
  masterKey: ` + validMasterKey + `
  accountsN: 4
  fundPerAccount: "0.01"
` + fundingExtra + `
governance:
  mode: preconfigured
workload:
  lanes:
    evm:
      targetInflight: 10
` + workloadExtra
}

func TestLoadAppliesWorkloadDefaults(t *testing.T) {
	target, err := Load(writeTarget(t, validTargetYAML("", "")))
	if err != nil {
		t.Fatal(err)
	}
	if target.Workload.Workers != 256 {
		t.Fatalf("workers=%d want 256", target.Workload.Workers)
	}
	if target.Workload.TargetTPS != 0 {
		t.Fatalf("targetTPS=%d want 0", target.Workload.TargetTPS)
	}
}

func TestFundingShouldSweepDefaults(t *testing.T) {
	if !((Funding{}).ShouldSweep()) {
		t.Fatalf("random funding should sweep by default")
	}
	if (Funding{AccountSeed: strings.Repeat("aa", 32)}).ShouldSweep() {
		t.Fatalf("seeded funding should retain by default")
	}
	on := true
	if !(Funding{AccountSeed: strings.Repeat("aa", 32), SweepBack: &on}).ShouldSweep() {
		t.Fatalf("explicit sweepBack=true should override seeded default")
	}
}

func TestLoadValidationRejectsUnsafeConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing master",
			yaml: strings.Replace(validTargetYAML("", ""), "  masterKey: "+validMasterKey+"\n", "", 1),
			want: "funding.masterKey is required",
		},
		{
			name: "zero accounts",
			yaml: strings.Replace(validTargetYAML("", ""), "  accountsN: 4", "  accountsN: 0", 1),
			want: "funding.accountsN must be > 0",
		},
		{
			name: "zero fund amount",
			yaml: strings.Replace(validTargetYAML("", ""), `  fundPerAccount: "0.01"`, `  fundPerAccount: "0"`, 1),
			want: "funding.fundPerAccount must be > 0",
		},
		{
			name: "short seed",
			yaml: validTargetYAML("  accountSeed: abcd\n", ""),
			want: "funding.accountSeed must be at least 32 bytes",
		},
		{
			name: "manifest without seed",
			yaml: validTargetYAML("  accountsFile: accounts.json\n", ""),
			want: "funding.accountsFile requires funding.accountSeed",
		},
		{
			name: "bad workers",
			yaml: validTargetYAML("", "  workers: 5000\n"),
			want: "workload.workers must be between 1 and 4096",
		},
		{
			name: "bad tps",
			yaml: validTargetYAML("", "  targetTPS: -1\n"),
			want: "workload.targetTPS must be between",
		},
		{
			name: "negative lane",
			yaml: strings.Replace(validTargetYAML("", ""), "      targetInflight: 10", "      targetInflight: -1", 1),
			want: "targetInflight must be >= 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeTarget(t, tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err=%v want containing %q", err, tt.want)
			}
		})
	}
}
