package api

import (
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
)

// mountPProf adds the standard Go pprof endpoints under /debug/pprof/.
// Opt-in via the PPROF_ENABLED env var because:
//   - /debug/pprof/heap and /goroutine are cheap to read and harmless
//   - /debug/pprof/profile triggers a 30s CPU profile that has a real
//     runtime cost during collection (recursive sampler)
//   - /debug/pprof/* exposes internal state we don't want public in production
//
// Use behind a private network or VPN. The docker-compose stack binds
// the host port to 127.0.0.1 so this is only loopback-reachable in dev.
func mountPProf(r chi.Router) {
	r.Route("/debug/pprof", func(p chi.Router) {
		p.Get("/", http.HandlerFunc(pprof.Index))
		p.Get("/cmdline", http.HandlerFunc(pprof.Cmdline))
		p.Get("/profile", http.HandlerFunc(pprof.Profile))
		p.Get("/symbol", http.HandlerFunc(pprof.Symbol))
		p.Post("/symbol", http.HandlerFunc(pprof.Symbol))
		p.Get("/trace", http.HandlerFunc(pprof.Trace))
		// Named subpaths: heap, goroutine, allocs, block, mutex, threadcreate, ...
		p.Get("/{name}", func(w http.ResponseWriter, r *http.Request) {
			pprof.Handler(chi.URLParam(r, "name")).ServeHTTP(w, r)
		})
	})
}
