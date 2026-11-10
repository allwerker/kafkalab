// Package web embeds the controlboard SPA (controlboard/web/dist, copied here by
// `make web-embed`). A placeholder index.html is committed so the backend always builds.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var dist embed.FS

// Dist returns the embedded build output.
func Dist() fs.FS {
	sub, _ := fs.Sub(dist, "dist")
	return sub
}

// Handler serves the SPA from files: existing files as they are (hashed assets cached for a
// year), every other path that does not look like a file gets index.html so client-side
// routes like /experiments/exp-1 survive a reload.
func Handler(files fs.FS) http.Handler {
	server := http.FileServerFS(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name != "" && name != "index.html" {
			if st, err := fs.Stat(files, name); err == nil && !st.IsDir() {
				if strings.HasPrefix(name, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				server.ServeHTTP(w, r)
				return
			}
			if path.Ext(name) != "" {
				http.NotFound(w, r) // a missing asset must not turn into HTML
				return
			}
		}
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeFileFS(w, r, files, "index.html")
	})
}
