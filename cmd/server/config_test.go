package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/data"
)

func TestAppConfigWiring(t *testing.T) {
	dir := t.TempDir()
	config, err := data.LoadConfig([]string{"-", "--port", "6382", "--pprof-addr", "127.0.0.1:7000"},
		strings.NewReader("port 6381\nbind 127.0.0.1 -::1\nappendonly yes\nappendfilename test.aof\ndir \""+dir+"\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	app, err := NewApp(config)
	if err != nil {
		t.Fatal(err)
	}
	defer app.database.Close()
	if app.respsrv.Addr != "127.0.0.1:6382" || !reflect.DeepEqual(app.respsrv.AdditionalAddresses, []string{"-[::1]:6382"}) || app.httpsrv.Addr != "127.0.0.1:7000" {
		t.Fatalf("configuration was not wired: %+v, %+v", app.respsrv, app.httpsrv)
	}
	if _, err := os.Stat(filepath.Join(dir, "appendonlydir", "test.aof.manifest")); err != nil {
		t.Fatal(err)
	}
}

func TestDisabledTCPDoesNotOpenAOF(t *testing.T) {
	for _, directive := range []string{"port 0", "bind \"\""} {
		dir := t.TempDir()
		config, err := data.LoadConfig([]string{"-"}, strings.NewReader(directive+"\nappendonly yes\ndir \""+dir+"\"\n"))
		if err != nil {
			t.Fatal(err)
		}
		if app, err := NewApp(config); app != nil || err == nil {
			t.Fatalf("disabled TCP started: %v, %v", app, err)
		}
		if _, err := os.Stat(config.AOFPath()); !os.IsNotExist(err) {
			t.Fatalf("invalid startup opened AOF: %v", err)
		}
	}
}
