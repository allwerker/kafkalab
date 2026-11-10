package pg

import (
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
	"testing/fstest"
)

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func testLogger(t *testing.T) *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// brokenFS lists files but cannot read them (e.g. permissions changed under a running pod).
type brokenFS struct{ fstest.MapFS }

func (b brokenFS) Open(name string) (fs.File, error) {
	if name == "." {
		return b.MapFS.Open(name)
	}
	return nil, errors.New("permission denied")
}

func (b brokenFS) ReadDir(name string) ([]fs.DirEntry, error) { return b.MapFS.ReadDir(name) }

// globErrFS fails to list its directory.
type globErrFS struct{}

func (globErrFS) Open(string) (fs.File, error)  { return nil, errors.New("io error") }
func (globErrFS) Glob(string) ([]string, error) { return nil, errors.New("io error") }

func (b brokenFS) ReadFile(string) ([]byte, error) { return nil, errors.New("permission denied") }
