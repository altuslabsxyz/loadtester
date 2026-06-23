package cmd

import (
	"os"
	"path/filepath"
)

// configDir is where loadtester keeps its settings: ~/.loadtester.
func configDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".loadtester"
	}
	return filepath.Join(home, ".loadtester")
}

// defaultTargetPath is the default --target: ~/.loadtester/target.yaml. Falls
// back to a relative path if the home directory cannot be resolved.
func defaultTargetPath() string {
	return filepath.Join(configDir(), "target.yaml")
}
