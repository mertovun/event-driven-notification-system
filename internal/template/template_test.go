package template

import (
	"errors"
	"sort"
	"testing"
)

func TestParseExtractsVars(t *testing.T) {
	cases := []struct {
		name string
		body string
		want []string
	}{
		{"no vars", "hello world", []string{}},
		{"single var", "Hi {{ .Name }}", []string{"Name"}},
		{"multiple vars", "{{ .Greeting }}, {{ .Name }}!", []string{"Greeting", "Name"}},
		{"duplicate dedup", "{{ .X }} and {{ .X }} again", []string{"X"}},
		{"sorted output", "{{ .Z }} {{ .A }} {{ .M }}", []string{"A", "M", "Z"}},
		{"underscore identifier", "{{ .first_name }}", []string{"first_name"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, vars, err := Parse(tc.body)
			if err != nil {
				t.Fatalf("Parse(%q) error: %v", tc.body, err)
			}
			if !sort.StringsAreSorted(vars) {
				t.Errorf("vars not sorted: %v", vars)
			}
			if !equalSlices(vars, tc.want) {
				t.Errorf("Parse(%q) vars = %v, want %v", tc.body, vars, tc.want)
			}
		})
	}
}

func TestParseInvalidReturnsErrParse(t *testing.T) {
	_, _, err := Parse("{{ .Unclosed ")
	if err == nil {
		t.Fatal("expected error for malformed template")
	}
	if !errors.Is(err, ErrParse) {
		t.Errorf("expected ErrParse, got %v", err)
	}
}

func TestRender(t *testing.T) {
	cases := []struct {
		name string
		body string
		vars map[string]any
		want string
	}{
		{"no vars", "static text", nil, "static text"},
		{"single substitution", "Hello {{ .Name }}", map[string]any{"Name": "Mert"}, "Hello Mert"},
		{"multiple substitutions", "{{ .A }}-{{ .B }}", map[string]any{"A": "x", "B": "y"}, "x-y"},
		{"numeric value", "code: {{ .N }}", map[string]any{"N": 42}, "code: 42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Render(tc.body, tc.vars)
			if err != nil {
				t.Fatalf("Render error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Render = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRenderMissingVarReturnsErrRender(t *testing.T) {
	_, err := Render("Hello {{ .Name }}", map[string]any{})
	if err == nil {
		t.Fatal("expected error for missing var")
	}
	if !errors.Is(err, ErrRender) {
		t.Errorf("expected ErrRender, got %v", err)
	}
}

func TestRenderParseErrorPropagates(t *testing.T) {
	_, err := Render("{{ .Unclosed ", map[string]any{"Unclosed": "x"})
	if !errors.Is(err, ErrParse) {
		t.Errorf("expected ErrParse to propagate through Render, got %v", err)
	}
}

func TestCheckRequired(t *testing.T) {
	cases := []struct {
		name     string
		required []string
		vars     map[string]any
		want     []string
	}{
		{"all present", []string{"A", "B"}, map[string]any{"A": 1, "B": 2}, []string{}},
		{"one missing", []string{"A", "B"}, map[string]any{"A": 1}, []string{"B"}},
		{"all missing", []string{"A", "B"}, map[string]any{}, []string{"A", "B"}},
		{"none required", []string{}, map[string]any{"A": 1}, []string{}},
		{"nil vars", []string{"A"}, nil, []string{"A"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CheckRequired(tc.required, tc.vars)
			if !equalSlices(got, tc.want) {
				t.Errorf("CheckRequired = %v, want %v", got, tc.want)
			}
		})
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
