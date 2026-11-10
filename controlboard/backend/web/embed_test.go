package web

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

var build = fstest.MapFS{
	"index.html":         {Data: []byte("<html>board</html>")},
	"assets/app-1a2b.js": {Data: []byte("console.log('board')")},
	"favicon.svg":        {Data: []byte("<svg/>")},
}

func get(path string) (int, string, string) {
	rec := httptest.NewRecorder()
	Handler(build).ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	b, _ := io.ReadAll(rec.Body)
	return rec.Code, string(b), rec.Header().Get("Cache-Control")
}

func Test_shouldServeTheAppShellWhenTheBrowserOpensTheBoardOrAClientRoute(t *testing.T) {
	// GIVEN: a built SPA
	// WHEN: the root and a deep client route are opened
	rootCode, root, rootCache := get("/")
	deepCode, deep, _ := get("/experiments/exp-1")

	// THEN: both get index.html, never cached
	if rootCode != 200 || deepCode != 200 || !strings.Contains(root, "board") || root != deep || rootCache != "no-cache" {
		t.Fatalf("root %d %q, deep %d %q", rootCode, root, deepCode, deep)
	}
}

func Test_shouldServeAssetsWithLongCachingWhenTheyExist(t *testing.T) {
	// GIVEN: a built SPA with a hashed bundle and a favicon
	// WHEN: both are requested
	jsCode, js, jsCache := get("/assets/app-1a2b.js")
	iconCode, _, iconCache := get("/favicon.svg")

	// THEN: they are served; only the hashed asset is cached for a year
	if jsCode != 200 || !strings.Contains(js, "console.log") || !strings.Contains(jsCache, "immutable") || iconCode != 200 || iconCache != "" {
		t.Fatalf("js %d %q, icon %d %q", jsCode, jsCache, iconCode, iconCache)
	}
}

func Test_shouldAnswerNotFoundWhenAMissingAssetIsRequested(t *testing.T) {
	// GIVEN: a built SPA after a redeploy (old bundle names are gone)
	// WHEN: an old bundle and the assets directory are requested
	oldCode, _, _ := get("/assets/app-old.js")
	dirCode, dir, _ := get("/assets")

	// THEN: the old bundle is a 404 instead of HTML; the directory falls back to the app shell
	if oldCode != 404 || dirCode != 200 || !strings.Contains(dir, "board") {
		t.Fatalf("old %d, dir %d", oldCode, dirCode)
	}
}

func Test_shouldEmbedAnIndexPageWhenTheBackendIsBuilt(t *testing.T) {
	// GIVEN: the embedded build (placeholder or real UI)
	// WHEN: the board root is served from it
	rec := httptest.NewRecorder()
	Handler(Dist()).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	// THEN: an HTML page is returned
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "<html") {
		t.Fatalf("got %d", rec.Code)
	}
}
