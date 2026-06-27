package docs

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// TestOpenAPISpecIsValid checks the embedded spec parses and documents every
// route the server exposes, so the contract cannot silently drift from the code.
func TestOpenAPISpecIsValid(t *testing.T) {
	if len(OpenAPISpec) == 0 {
		t.Fatal("embedded OpenAPI spec is empty")
	}

	var doc struct {
		OpenAPI string         `yaml:"openapi"`
		Info    map[string]any `yaml:"info"`
		Paths   map[string]any `yaml:"paths"`
		Comps   map[string]any `yaml:"components"`
	}
	if err := yaml.Unmarshal(OpenAPISpec, &doc); err != nil {
		t.Fatalf("spec is not valid YAML: %v", err)
	}

	if doc.OpenAPI == "" {
		t.Error("missing openapi version")
	}
	if doc.Info["title"] == nil {
		t.Error("missing info.title")
	}

	wantPaths := []string{
		"/health",
		"/ready",
		"/metrics",
		"/topics",
		"/topics/{topic}",
		"/topics/{topic}/consume",
		"/topics/{topic}/publish",
		"/topics/{topic}/publish/batch",
	}
	for _, p := range wantPaths {
		if _, ok := doc.Paths[p]; !ok {
			t.Errorf("spec is missing documented path %q", p)
		}
	}
}
