package data

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config contains only settings used by Peridot. Other Redis directives are
// ignored, including module configuration and unsupported persistence options.
type Config struct {
	Bind                    []string
	Port                    int
	HZ                      int
	DynamicHZ               bool
	AppendOnly              bool
	AppendFsync             string
	AppendFilename          string
	AppendDirname           string
	AOFUseRDBPreamble       bool
	DBFilename              string
	Save                    []SaveRule
	StopWritesOnBGSaveError bool
	Dir                     string
	Extra                   Extra
}

// SaveRule triggers a snapshot after Seconds and at least Changes mutations.
type SaveRule struct {
	Seconds int64
	Changes uint64
}

// Extra holds Peridot-specific settings that are accepted only on the CLI.
type Extra struct {
	PprofAddress string
}

func DefaultConfig() Config {
	return Config{
		Bind: []string{"*", "-::*"}, Port: 6379,
		HZ: 10, DynamicHZ: true,
		AppendFilename: "appendonly.aof", Dir: ".",
		AppendFsync:   "everysec",
		AppendDirname: "appendonlydir", AOFUseRDBPreamble: true,
		DBFilename: "dump.rdb", Save: []SaveRule{{3600, 1}, {300, 100}, {60, 10000}}, StopWritesOnBGSaveError: true,
		Extra: Extra{PprofAddress: "localhost:6060"},
	}
}

// AOFPath selects the legacy single-file AOF used only for migration.
func (c Config) AOFPath() string {
	if !c.AppendOnly {
		return ""
	}
	return filepath.Join(c.Dir, c.AppendFilename)
}

// LoadConfig reads an optional first positional config file, optional stdin
// ("-" as the first or last argument), then --directive arguments in order.
// Includes and relative dir values resolve against Redis's logical working
// directory, initially the process working directory. No process chdir occurs.
func LoadConfig(args []string, stdin io.Reader) (Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return Config{}, err
	}
	p := configParser{config: DefaultConfig(), cwd: cwd}
	var filename string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		filename, args = args[0], args[1:]
	}
	readStdin := false
	if filename == "" && len(args) > 0 && args[0] == "-" {
		readStdin, args = true, args[1:]
	}
	if len(args) > 0 && args[len(args)-1] == "-" {
		readStdin, args = true, args[:len(args)-1]
	}
	if filename != "" {
		if err := p.file(filename); err != nil {
			return Config{}, err
		}
	}
	if readStdin {
		if stdin == nil {
			return Config{}, errors.New("stdin configuration requested without a reader")
		}
		if err := p.read(stdin, "stdin"); err != nil {
			return Config{}, err
		}
	}
	for len(args) > 0 {
		if !strings.HasPrefix(args[0], "--") {
			return Config{}, fmt.Errorf("expected --directive, got %q", args[0])
		}
		// Redis also accepts a whole directive in one shell argument.
		words, err := splitConfigLine(args[0][2:])
		if err != nil || len(words) == 0 {
			return Config{}, fmt.Errorf("invalid CLI directive %q", args[0])
		}
		args = args[1:]
		for len(args) > 0 {
			if strings.HasPrefix(args[0], "--") && (len(words) > 1 || words[0] == "save" || words[0] == "sentinel") {
				break
			}
			words, args = append(words, args[0]), args[1:]
		}
		if err := p.apply(words, true); err != nil {
			return Config{}, fmt.Errorf("CLI --%s: %w", words[0], err)
		}
	}
	return p.config, nil
}

type configParser struct {
	config  Config
	cwd     string
	depth   int
	saveSet bool
}

func (p *configParser) file(name string) error {
	if p.depth >= 128 {
		return errors.New("config include nesting exceeds 128 levels")
	}
	if !filepath.IsAbs(name) {
		name = filepath.Join(p.cwd, name)
	}
	names := []string{name}
	if strings.ContainsAny(name, "*?[") {
		var err error
		names, err = filepath.Glob(name)
		if err != nil {
			return fmt.Errorf("include pattern: %w", err)
		}
	}
	p.depth++
	defer func() { p.depth-- }()
	for _, name := range names {
		f, err := os.Open(name)
		if err != nil {
			return fmt.Errorf("open config: %w", err)
		}
		err = p.read(f, name)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}

func (p *configParser) read(r io.Reader, source string) error {
	rd := bufio.NewReader(r)
	for line := 1; ; line++ {
		s, readErr := rd.ReadString('\n')
		s = strings.Trim(s, " \t\r\n")
		if s != "" && !strings.HasPrefix(s, "#") {
			words, err := splitConfigLine(s)
			if err == nil && len(words) > 0 {
				err = p.apply(words, false)
			}
			if err != nil {
				return fmt.Errorf("%s:%d: %w", source, line, err)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return fmt.Errorf("%s:%d: %w", source, line, readErr)
		}
	}
}

func (p *configParser) apply(words []string, cli bool) error {
	name, values := strings.ToLower(words[0]), words[1:]
	switch name {
	case "save":
		if !p.saveSet {
			p.config.Save = nil
			p.saveSet = true
		}
		if len(values) == 1 && values[0] == "" {
			p.config.Save = nil
			return nil
		}
		if len(values) == 0 || len(values)%2 != 0 {
			return errors.New("save requires seconds/changes pairs or an empty string")
		}
		for i := 0; i < len(values); i += 2 {
			seconds, err := strconv.ParseInt(values[i], 10, 64)
			changes, changeErr := strconv.ParseUint(values[i+1], 10, 64)
			if err != nil || changeErr != nil || seconds < 1 {
				return errors.New("invalid save seconds or changes")
			}
			p.config.Save = append(p.config.Save, SaveRule{seconds, changes})
		}
		return nil
	case "bind":
		if len(values) == 1 {
			var err error
			values, err = splitConfigLine(values[0])
			if err != nil {
				return err
			}
		}
		if len(values) > 16 {
			return errors.New("bind accepts at most 16 addresses")
		}
		p.config.Bind = append([]string{}, values...)
		return nil
	case "port", "hz", "dynamic-hz", "appendonly", "appendfsync", "appendfilename", "appenddirname", "dbfilename", "aof-use-rdb-preamble", "stop-writes-on-bgsave-error", "dir", "include", "pprof-addr":
		if name == "pprof-addr" && !cli {
			return errors.New("pprof-addr is CLI-only and cannot appear in redis.conf")
		}
		if len(values) != 1 {
			return fmt.Errorf("%s requires exactly one argument", name)
		}
	default:
		return nil
	}
	v := values[0]
	switch name {
	case "hz":
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 0 {
			return errors.New("hz must be an integer between 0 and 2147483647")
		}
		// Redis accepts nonnegative int32 values and clamps the baseline.
		p.config.HZ = int(max(1, min(500, n)))
	case "port":
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 65535 {
			return errors.New("port must be an integer between 0 and 65535")
		}
		p.config.Port = n
	case "dynamic-hz", "appendonly", "aof-use-rdb-preamble", "stop-writes-on-bgsave-error":
		if !strings.EqualFold(v, "yes") && !strings.EqualFold(v, "no") {
			return fmt.Errorf("%s must be yes or no", name)
		}
		enabled := strings.EqualFold(v, "yes")
		switch name {
		case "dynamic-hz":
			p.config.DynamicHZ = enabled
		case "appendonly":
			p.config.AppendOnly = enabled
		case "aof-use-rdb-preamble":
			p.config.AOFUseRDBPreamble = enabled
		case "stop-writes-on-bgsave-error":
			p.config.StopWritesOnBGSaveError = enabled
		}
	case "appendfsync":
		switch strings.ToLower(v) {
		case "always", "everysec", "no":
			p.config.AppendFsync = strings.ToLower(v)
		default:
			return errors.New("appendfsync must be always, everysec, or no")
		}
	case "appendfilename", "appenddirname", "dbfilename":
		if v == "" || v == "." || v == ".." || strings.ContainsAny(v, "/\x00") {
			return fmt.Errorf("%s must be a nonempty filename without a path", name)
		}
		switch name {
		case "appendfilename":
			p.config.AppendFilename = v
		case "appenddirname":
			p.config.AppendDirname = v
		case "dbfilename":
			p.config.DBFilename = v
		}
	case "dir":
		if v == "" {
			return errors.New("dir cannot be empty")
		}
		if !filepath.IsAbs(v) {
			v = filepath.Join(p.cwd, v)
		}
		info, err := os.Stat(v)
		if err != nil {
			return fmt.Errorf("dir: %w", err)
		}
		if !info.IsDir() {
			return errors.New("dir must name a directory")
		}
		p.cwd, p.config.Dir = v, v
	case "include":
		return p.file(v)
	case "pprof-addr":
		p.config.Extra.PprofAddress = v
	}
	return nil
}

// splitConfigLine follows Redis quoting, rather than shell quoting: only full
// comment lines are comments; bare backslashes are literal; single quotes only
// unescape \'; double quotes support byte escapes and \xHH.
func splitConfigLine(s string) ([]string, error) {
	var words []string
	for i := 0; i < len(s); {
		if configSpace(s[i]) {
			i++
			continue
		}
		var word strings.Builder
		var quote byte
		for i < len(s) {
			c := s[i]
			if quote == 0 {
				if configSpace(c) {
					break
				}
				if c == '\'' || c == '"' {
					quote = c
					i++
					continue
				}
			} else {
				if c == quote {
					i++
					if i < len(s) && !configSpace(s[i]) {
						return nil, errors.New("closing quote must be followed by whitespace")
					}
					quote = 0
					break
				}
				if c == '\\' && i+1 < len(s) {
					next := s[i+1]
					if quote == '\'' && next == '\'' {
						c, i = next, i+1
					} else if quote == '"' {
						c, i = next, i+1
						if next == 'x' && i+2 < len(s) {
							if n, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
								c, i = byte(n), i+2
							}
						} else {
							switch next {
							case 'n':
								c = '\n'
							case 'r':
								c = '\r'
							case 't':
								c = '\t'
							case 'b':
								c = '\b'
							case 'a':
								c = '\a'
							}
						}
					}
				}
			}
			word.WriteByte(c)
			i++
		}
		if quote != 0 {
			return nil, errors.New("unbalanced quotes in configuration line")
		}
		words = append(words, word.String())
	}
	return words, nil
}

func configSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\v' || c == '\f'
}

// SplitArguments parses Redis byte quoting, shared by configuration and AOF manifests.
func SplitArguments(s string) ([]string, error) { return splitConfigLine(s) }
