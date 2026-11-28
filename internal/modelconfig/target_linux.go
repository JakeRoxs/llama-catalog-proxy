//go:build linux

package modelconfig

import (
	"errors"
	"os"
	"syscall"
)

func openTargetFile(root *os.Root, name string) (*os.File, error) {
	file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("management target unavailable or unsafe")
	}
	return file, nil
}
