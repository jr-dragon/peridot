package data

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestExpirationConfig(t *testing.T) {
	defaults := DefaultConfig()
	if defaults.HZ != 10 || !defaults.DynamicHZ {
		t.Fatal("expiration defaults", defaults)
	}
	for _, tc := range []struct {
		input   string
		hz      int
		dynamic bool
	}{
		{"hz 1\ndynamic-hz no", 1, false},
		{"hz 500\ndynamic-hz YES", 500, true},
		{"hz 0", 1, true},
		{"hz 501", 500, true},
		{"hz 2147483647", 500, true},
		{"hz 30\nhz 40\ndynamic-hz no\ndynamic-hz yes", 40, true},
	} {
		got, err := LoadConfig([]string{"-"}, strings.NewReader(tc.input))
		if err != nil || got.HZ != tc.hz || got.DynamicHZ != tc.dynamic {
			t.Fatalf("%q: %+v, %v", tc.input, got, err)
		}
	}
	for _, input := range []string{"hz -1", "hz 2147483648", "hz 1.5", "hz garbage", "hz", "hz 10 20", "dynamic-hz", "dynamic-hz enabled", "dynamic-hz yes no"} {
		if _, err := LoadConfig([]string{"-"}, strings.NewReader(input)); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	dir := t.TempDir()
	child := filepath.Join(dir, "child.conf")
	file := filepath.Join(dir, "redis.conf")
	writeConfig(t, child, "hz 50\ndynamic-hz no\n")
	writeConfig(t, file, "hz 20\ninclude \""+child+"\"\n")
	got, err := LoadConfig([]string{file, "--hz", "75", "--dynamic-hz", "yes"}, nil)
	if err != nil || got.HZ != 75 || !got.DynamicHZ {
		t.Fatalf("CLI precedence: %+v, %v", got, err)
	}
}
