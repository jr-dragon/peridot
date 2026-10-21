package data

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestConfigDefaultsAndOverrides(t *testing.T) {
	got, err := LoadConfig(nil, nil)
	if err != nil || !reflect.DeepEqual(got, DefaultConfig()) {
		t.Fatalf("defaults = %+v, %v", got, err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "redis.conf")
	writeConfig(t, path, "# comment\r\nPoRt 6380\r\nport 6381\nbind 127.0.0.1 -::1\nappendonly YES\nappendfilename \"disk file.aof\"\ndir \""+dir+"\"\nmaxmemory nonsense\nloadmodule /nonexistent/module.so\nsearch.threads 5\n")
	got, err = LoadConfig([]string{path, "--port", "6382", "--bind", "127.0.0.2", "-::1", "--pprof-addr", "localhost:7000"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Port != 6382 || !reflect.DeepEqual(got.Bind, []string{"127.0.0.2", "-::1"}) || got.Extra.PprofAddress != "localhost:7000" || got.AOFPath() != filepath.Join(dir, "disk file.aof") {
		t.Fatalf("config = %+v", got)
	}
}

func TestConfigIncludesAndWorkingDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Mkdir("nested", 0700); err != nil {
		t.Fatal(err)
	}
	writeConfig(t, "redis-full.conf", "include redis.conf\nloadmodule missing.so\nport 6385\n")
	writeConfig(t, "redis.conf", "port 6380\ninclude fragments-*.conf\ninclude no-matches-*.conf\ndir nested\ninclude local.conf\n")
	writeConfig(t, "fragments-b.conf", "port 6382\n")
	writeConfig(t, "fragments-a.conf", "port 6381\n")
	writeConfig(t, "nested/local.conf", "port 6383\nappendonly yes\n")
	got, err := LoadConfig([]string{"redis-full.conf", "--port", "6386"}, nil)
	if err != nil || got.Port != 6386 || got.Dir != filepath.Join(dir, "nested") {
		t.Fatalf("config = %+v, %v", got, err)
	}
	cwd, _ := os.Getwd()
	if cwd != dir {
		t.Fatalf("parser changed process cwd to %s", cwd)
	}
	got, err = LoadConfig([]string{"redis.conf"}, nil)
	if err != nil || got.Port != 6383 {
		t.Fatalf("nested include = %+v, %v", got, err)
	}
	got, err = LoadConfig([]string{"--include", "fragments-*.conf"}, nil)
	if err != nil || got.Port != 6382 {
		t.Fatalf("glob order = %+v, %v", got, err)
	}
	writeConfig(t, "nested/entry.conf", "include from-cwd.conf\n")
	writeConfig(t, "from-cwd.conf", "port 6387\n")
	got, err = LoadConfig([]string{"nested/entry.conf"}, nil)
	if err != nil || got.Port != 6387 {
		t.Fatalf("cwd-relative include = %+v, %v", got, err)
	}
}

func TestConfigStdinAndCLI(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		port     int
		bind     []string
		filename string
	}{
		{"stdin first", []string{"-", "--port", "6382"}, 6382, nil, ""},
		{"stdin last", []string{"--port", "6382", "-"}, 6382, nil, ""},
		{"whole directive", []string{"--port 6383"}, 6383, nil, ""},
		{"quoted bind list", []string{"--bind", "127.0.0.1 -::1"}, 6379, []string{"127.0.0.1", "-::1"}, ""},
		{"empty bind", []string{"--bind", ""}, 6379, []string{}, ""},
		{"zero port", []string{"--port", "0"}, 0, nil, ""},
		{"literal CLI bytes", []string{"--appendfilename", "a\\b\"c.aof"}, 6379, nil, "a\\b\"c.aof"},
		{"option-like value", []string{"--appendfilename", "--port", "--port", "6384"}, 6384, nil, "--port"},
		{"ignored directives", []string{"--save", "", "--port", "6385", "--loadmodule", "missing.so"}, 6385, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LoadConfig(tc.args, strings.NewReader("port 6381\n"))
			if err != nil {
				t.Fatal(err)
			}
			if got.Port != tc.port || (tc.bind != nil && !reflect.DeepEqual(got.Bind, tc.bind)) || (tc.filename != "" && got.AppendFilename != tc.filename) {
				t.Fatalf("config = %+v", got)
			}
		})
	}
	path := filepath.Join(t.TempDir(), "redis.conf")
	writeConfig(t, path, "port 6380\n")
	got, err := LoadConfig([]string{path, "-"}, strings.NewReader("port 6381\n"))
	if err != nil || got.Port != 6381 {
		t.Fatalf("file then stdin = %+v, %v", got, err)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, line := range []string{"port", "port 65536", "port -1", "port invalid", "port 6380 # not an inline comment", "appendonly true", "appendfsync", "appendfsync invalid", "appendfsync everysec extra", "appendfilename \"\"", "appendfilename sub/file.aof", "dir /nonexistent-peridot-dir", "dir \"\"", "include /nonexistent-peridot-config", "pprof-addr localhost:9000", "unknown \"unterminated", "appendfilename \"x\"suffix"} {
		t.Run(line, func(t *testing.T) {
			_, err := LoadConfig([]string{"-"}, strings.NewReader("# first line\n"+line))
			if err == nil || !strings.Contains(err.Error(), "stdin:2:") {
				t.Fatalf("expected line error, got %v", err)
			}
		})
	}
	for _, args := range [][]string{{"--port"}, {"--port", "6380", "stray"}, {"--"}, {"-", "-"}} {
		if _, err := LoadConfig(args, nil); err == nil {
			t.Fatalf("accepted invalid CLI %q", args)
		}
	}
	_, err := LoadConfig([]string{"/nonexistent-peridot-config"}, nil)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lost file error identity: %v", err)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	writeConfig(t, "cycle.conf", "include cycle.conf\n")
	if _, err := LoadConfig([]string{"cycle.conf"}, nil); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("include cycle = %v", err)
	}
}

func TestSplitConfigLine(t *testing.T) {
	for _, tc := range []struct {
		line string
		want []string
	}{
		{`name "a\n\r\t\b\a\x41\xff\q\xZZ"`, []string{"name", "a\n\r\t\b\aA\xffqxZZ"}},
		{`name 'a\nb\'c'`, []string{"name", "a\\nb'c"}},
		{`name a\b prefix" with spaces"`, []string{"name", "a\\b", "prefix with spaces"}},
		{`name "" '#literal' #literal`, []string{"name", "", "#literal", "#literal"}},
	} {
		got, err := splitConfigLine(tc.line)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("split(%q) = %q, %v; want %q", tc.line, got, err, tc.want)
		}
	}
	// Unsupported long values must not inherit bufio.Scanner's 64 KiB limit.
	got, err := LoadConfig([]string{"-"}, strings.NewReader("ignored \""+strings.Repeat("x", 100000)+"\"\nport 6389"))
	if err != nil || got.Port != 6389 {
		t.Fatalf("long line = %+v, %v", got, err)
	}
}

// Point PERIDOT_REDIS_CONFIG_DIR at unmodified official samples, organized as
// <version>/redis.conf and <version>/redis-full.conf. No network is used in tests.
func TestOfficialRedisConfigs(t *testing.T) {
	root := os.Getenv("PERIDOT_REDIS_CONFIG_DIR")
	if root == "" {
		t.Skip("set PERIDOT_REDIS_CONFIG_DIR to test official Redis samples")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"8.0", "8.2", "8.4", "8.6", "8.8", "8.10", "8.10.2"} {
		t.Run(version, func(t *testing.T) {
			t.Chdir(filepath.Join(root, version))
			names := []string{"redis.conf"}
			if version != "8.10" && version != "8.10.2" {
				names = append(names, "redis-full.conf")
			}
			for _, name := range names {
				got, err := LoadConfig([]string{name}, nil)
				if err != nil {
					t.Fatal(err)
				}
				if got.Port != 6379 || !reflect.DeepEqual(got.Bind, []string{"127.0.0.1", "-::1"}) || got.AppendOnly || got.AppendFilename != "appendonly.aof" {
					t.Fatalf("%s: %+v", name, got)
				}
			}
		})
	}
}

func TestConfigAppendFsync(t *testing.T) {
	for _, policy := range []string{"no", "always", "everysec"} {
		t.Run(policy, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "redis.conf")
			writeConfig(t, path, "appendonly yes\nappendfsync "+strings.ToUpper(policy)+"\n")
			got, err := LoadConfig([]string{path}, nil)
			if err != nil || got.AppendFsync != policy || !got.AppendOnly {
				t.Fatalf("file = %+v, %v", got, err)
			}
			got, err = LoadConfig([]string{path, "--appendfsync", "everysec", "--appendonly", "no"}, nil)
			if err != nil || got.AppendFsync != "everysec" || got.AppendOnly {
				t.Fatalf("CLI override = %+v, %v", got, err)
			}
			got, err = LoadConfig([]string{"-", "--appendfsync", policy}, strings.NewReader("appendfsync always\n"))
			if err != nil || got.AppendFsync != policy {
				t.Fatalf("stdin override = %+v, %v", got, err)
			}
		})
	}
	if got := DefaultConfig(); got.AppendFsync != "everysec" || got.AppendOnly {
		t.Fatalf("defaults = %+v", got)
	}
	for _, args := range [][]string{{"--appendfsync"}, {"--appendfsync", "invalid"}, {"--appendfsync", "no", "extra"}} {
		if _, err := LoadConfig(args, nil); err == nil {
			t.Fatalf("accepted %q", args)
		}
	}
}
