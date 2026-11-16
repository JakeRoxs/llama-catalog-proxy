package proxy

import (
	"embed"
	"net/http"
	"path"
)

//go:embed benchmark_ui/admin.html benchmark_ui/admin.css benchmark_ui/admin.js
var adminAssets embed.FS

func serveAdminAsset(w http.ResponseWriter, req *http.Request, name string) {
	if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.URL.RawPath != "" || path.Base(name) != name {
		http.NotFound(w, req)
		return
	}
	data, err := adminAssets.ReadFile("benchmark_ui/" + name)
	if err != nil {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	switch name {
	case "admin.html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case "admin.css":
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case "admin.js":
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	default:
		http.NotFound(w, req)
		return
	}
	_, _ = w.Write(data)
}
