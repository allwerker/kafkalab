package serde

import (
	"os"
	"path/filepath"

	"kafkalab/pkg/kit/testkit"
)

func readSchema(name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(testkit.RepoRoot(), "schemas", "gen", name+".avsc"))
	return string(b), err
}
