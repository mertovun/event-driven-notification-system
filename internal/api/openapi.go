package api

import (
	_ "embed"
	"net/http"
)

// openAPIYAML is the embedded OpenAPI 3.1 spec. Single source of truth at api/openapi.yaml.
// We embed it so a docker container is self-contained — no host bind-mount needed.
//
//go:embed openapi.yaml
var openAPIYAML []byte

// swaggerHTML is a minimal Swagger UI page that loads the Swagger UI bundle from a
// CDN and points at our embedded /openapi.yaml. Trade-off: pulling the UI
// from a CDN keeps the binary small and the implementation tiny.
// For offline review, swap to `go:embed swagger-ui-dist/`.
const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Event-Driven Notification System — API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      SwaggerUIBundle({
        url: "/openapi.yaml",
        dom_id: "#swagger-ui",
        presets: [SwaggerUIBundle.presets.apis],
      });
    };
  </script>
</body>
</html>
`

// openAPIHandler returns the embedded OpenAPI 3.1 spec as application/yaml.
func openAPIHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	_, _ = w.Write(openAPIYAML)
}

// swaggerHandler returns the Swagger UI HTML page.
func swaggerHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerHTML))
}
