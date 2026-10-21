//go:build !darwin && !linux && !freebsd && !openbsd && !netbsd && !dragonfly

package persistence

import (
	"errors"
	"os"
)

func validateAOFFile(appendFile) error {
	return errors.New("aof: file validation is unsupported on this platform")
}

func lockAOF(string) (*os.File, error) {
	return nil, errors.New("aof: file locking is unsupported on this platform")
}
func syncDirectory(string) error {
	return errors.New("aof: directory sync is unsupported on this platform")
}
