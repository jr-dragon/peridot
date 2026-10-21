package persistence

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jr-dragon/peridot/internal/data"
)

type segment struct {
	Name string
	Seq  int64
	Kind string
}
type manifest struct {
	Base    *segment
	Incr    []segment
	History []segment
}

func validFilename(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\x00")
}

func readManifest(r io.Reader) (manifest, error) {
	var m manifest
	rd := bufio.NewReaderSize(r, 1025)
	names := make(map[string]bool)
	lines := 0
	for {
		raw, err := rd.ReadSlice('\n')
		line := string(raw)
		if err == io.EOF && line == "" {
			break
		}
		if err != nil || len(line) > 1024 {
			return m, errors.New("aof: incomplete or oversized manifest line")
		}
		lines++
		if strings.HasPrefix(line, "#") {
			continue
		}
		words, err := data.SplitArguments(strings.TrimSpace(line))
		if err != nil || len(words) < 6 || len(words)%2 != 0 {
			return m, errors.New("aof: invalid manifest fields")
		}
		var s segment
		seen := make(map[string]bool)
		for i := 0; i < len(words); i += 2 {
			key := strings.ToLower(words[i])
			if seen[key] {
				return m, errors.New("aof: duplicate manifest field")
			}
			seen[key] = true
			switch key {
			case "file":
				s.Name = words[i+1]
			case "seq":
				s.Seq, err = strconv.ParseInt(words[i+1], 10, 64)
				if err != nil {
					return m, err
				}
			case "type":
				s.Kind = words[i+1]
			}
		}
		if !validFilename(s.Name) || s.Seq <= 0 || names[s.Name] {
			return m, errors.New("aof: invalid or duplicate manifest entry")
		}
		names[s.Name] = true
		switch s.Kind {
		case "b":
			if m.Base != nil {
				return m, errors.New("aof: duplicate base")
			}
			m.Base = &s
		case "i":
			if len(m.Incr) > 0 && s.Seq <= m.Incr[len(m.Incr)-1].Seq {
				return m, errors.New("aof: non-monotonic incremental sequence")
			}
			m.Incr = append(m.Incr, s)
		case "h":
			m.History = append(m.History, s)
		default:
			return m, errors.New("aof: unknown manifest entry type")
		}
	}
	if lines == 0 {
		return m, errors.New("aof: empty manifest")
	}
	return m, nil
}

// Redis uses byte escapes, not Go's Unicode escapes, in quoted filenames.
func quoteFilename(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '"' || c == '\\' {
			b.WriteByte('\\')
			b.WriteByte(c)
		} else if c < 32 || c >= 127 {
			fmt.Fprintf(&b, "\\x%02x", c)
		} else {
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}
func (m manifest) write(w io.Writer) error {
	all := m.files()
	for _, s := range all {
		if _, err := fmt.Fprintf(w, "file %s seq %d type %s\n", quoteFilename(s.Name), s.Seq, s.Kind); err != nil {
			return err
		}
	}
	return nil
}
func (m manifest) files() []segment {
	var all []segment
	if m.Base != nil {
		all = append(all, *m.Base)
	}
	all = append(all, m.History...)
	all = append(all, m.Incr...)
	return all
}
