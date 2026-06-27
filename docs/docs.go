// Package docs embeds the OpenAPI specification so it can be served by the
// application binary without depending on files on disk.
package docs

import _ "embed"

// OpenAPISpec is the raw OpenAPI 3 document describing the HTTP API.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
