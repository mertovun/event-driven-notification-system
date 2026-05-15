// Package template handles template parsing, variable extraction, and rendering
// at create time. See docs/12-template-system.md §3.
package template

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	texttmpl "text/template"
)

// ErrParse — template body did not parse.
var ErrParse = errors.New("template: parse failed")

// ErrRender — rendering failed (missing var, bad expression).
var ErrRender = errors.New("template: render failed")

// varRefRe is the practical match for `{{ .Foo }}` references. We accept dot-prefixed
// identifiers — anything more elaborate (e.g., method calls, pipelines) is allowed
// at parse/render time but won't be listed as a required variable here.
// See docs/12 §4 for the regex-shortcut rationale.
var varRefRe = regexp.MustCompile(`\{\{[^}]*\.([A-Za-z_][A-Za-z0-9_]*)[^}]*\}\}`)

// Parse compiles the body. On success returns the parsed template plus the set of
// variable references it discovered. The order is deterministic (sorted) so re-parses
// produce the same required_vars list.
func Parse(body string) (*texttmpl.Template, []string, error) {
	t, err := texttmpl.New("body").
		Option("missingkey=error"). // any undefined var → ErrRender at execute time
		Parse(body)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrParse, err)
	}

	// Extract referenced variables via the regex shortcut.
	seen := make(map[string]struct{})
	for _, m := range varRefRe.FindAllStringSubmatch(body, -1) {
		seen[m[1]] = struct{}{}
	}
	vars := make([]string, 0, len(seen))
	for v := range seen {
		vars = append(vars, v)
	}
	sort.Strings(vars)
	return t, vars, nil
}

// Render executes the template against the given variables and returns the rendered string.
// Returns ErrRender on missing-var or any execution error.
func Render(body string, variables map[string]any) (string, error) {
	t, _, err := Parse(body)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, variables); err != nil {
		return "", fmt.Errorf("%w: %v", ErrRender, err)
	}
	return buf.String(), nil
}

// CheckRequired returns a list of required-var names that are missing from variables.
func CheckRequired(required []string, variables map[string]any) []string {
	missing := make([]string, 0)
	for _, r := range required {
		if _, ok := variables[r]; !ok {
			missing = append(missing, r)
		}
	}
	return missing
}
