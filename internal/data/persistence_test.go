package data

import (
	"reflect"
	"strings"
	"testing"
)

func TestPersistenceConfig(t *testing.T) {
	got, err := LoadConfig([]string{"-", "--save", "3", "4", "--aof-use-rdb-preamble", "no"}, strings.NewReader("save 10 2\nsave 20 5\nappenddirname logs\ndbfilename snapshot.rdb\nstop-writes-on-bgsave-error no\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Save, []SaveRule{{10, 2}, {20, 5}, {3, 4}}) || got.AppendDirname != "logs" || got.DBFilename != "snapshot.rdb" || got.AOFUseRDBPreamble || got.StopWritesOnBGSaveError {
		t.Fatal(got)
	}
	got, err = LoadConfig([]string{"--save", "", "--save", "1", "0"}, nil)
	if err != nil || !reflect.DeepEqual(got.Save, []SaveRule{{1, 0}}) {
		t.Fatal(got, err)
	}
	for _, args := range [][]string{{"--save"}, {"--save", "1"}, {"--save", "0", "1"}, {"--save", "1", "-1"}, {"--appenddirname", "../logs"}, {"--dbfilename", "."}, {"--aof-use-rdb-preamble", "maybe"}} {
		if _, err := LoadConfig(args, nil); err == nil {
			t.Fatal("accepted", args)
		}
	}
}
