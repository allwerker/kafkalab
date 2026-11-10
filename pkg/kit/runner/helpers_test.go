package runner

import (
	"io/fs"
	"testing"
)

func mustSub(t *testing.T) fs.FS {
	t.Helper()
	sub, err := fs.Sub(Migrations(), "migrations")
	if err != nil {
		t.Fatal(err)
	}
	return sub
}
