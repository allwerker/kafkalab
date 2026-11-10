package testkit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"

	"kafkalab/pkg/contracts"
)

// Registry is an in-memory Confluent-compatible schema registry with the subset of the API
// the lab uses: latest version of a subject, schema by id, and registering a schema.
type Registry struct {
	URL string

	mu       sync.Mutex
	nextID   int
	subjects map[string][]subjectVersion
	byID     map[int]string
	Down     bool // when true every request fails with 503 (registry outage)
}

type subjectVersion struct {
	ID      int    `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
	Subject string `json:"subject"`
}

// NewRegistry starts a fake registry. With preload=true every topic's schema from schemas/gen
// is registered, exactly like the schema-registration job does in the cluster.
func NewRegistry(t testing.TB, preload bool) *Registry {
	t.Helper()
	r := &Registry{nextID: 1, subjects: map[string][]subjectVersion{}, byID: map[int]string{}}
	srv := httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(srv.Close)
	r.URL = srv.URL
	if preload {
		for topic, name := range contracts.ValueTypes {
			raw, err := os.ReadFile(filepath.Join(RepoRoot(), "schemas", "gen", name+".avsc"))
			if err != nil {
				t.Fatalf("read schema %s: %v", name, err)
			}
			r.Add(contracts.Subject(topic), string(raw))
		}
	}
	return r
}

// Client returns a franz-go schema registry client for this registry.
func (r *Registry) Client(t testing.TB) *sr.Client {
	cl, err := sr.NewClient(sr.URLs(r.URL))
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// Add registers schema under subject and returns its id.
func (r *Registry) Add(subject, schema string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	versions := r.subjects[subject]
	r.subjects[subject] = append(versions, subjectVersion{ID: id, Version: len(versions) + 1, Schema: schema, Subject: subject})
	r.byID[id] = schema
	return id
}

// SetDown simulates a registry outage.
func (r *Registry) SetDown(down bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Down = down
}

func (r *Registry) serve(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	if r.Down {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error_code":50301,"message":"registry down"}`))
		return
	}
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	switch {
	case len(parts) == 4 && parts[0] == "subjects" && parts[2] == "versions" && req.Method == http.MethodGet:
		versions := r.subjects[parts[1]]
		if len(versions) == 0 {
			notFound(w, 40401, "Subject not found")
			return
		}
		v := versions[len(versions)-1]
		if parts[3] != "latest" && parts[3] != "-1" {
			n, _ := strconv.Atoi(parts[3])
			if n < 1 || n > len(versions) {
				notFound(w, 40402, "Version not found")
				return
			}
			v = versions[n-1]
		}
		_ = json.NewEncoder(w).Encode(v)
	case len(parts) == 3 && parts[0] == "schemas" && parts[1] == "ids":
		id, _ := strconv.Atoi(parts[2])
		s, ok := r.byID[id]
		if !ok {
			notFound(w, 40403, "Schema not found")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"schema": s})
	default:
		notFound(w, 404, "unsupported in fake registry: "+req.Method+" "+req.URL.Path)
	}
}

func notFound(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]any{"error_code": code, "message": msg})
}

// RepoRoot returns the repository root (directory containing go.mod).
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
