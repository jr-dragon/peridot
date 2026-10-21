//go:build darwin || linux || freebsd || openbsd || netbsd || dragonfly

package persistence

import (
	"errors"
	"os"
	"syscall"
)

func validateAOFFile(f appendFile) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("aof: expected a regular file")
	}
	if info.Sys().(*syscall.Stat_t).Nlink != 1 {
		return errors.New("aof: file must have exactly one hard link")
	}
	return nil
}

func lockAOF(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
