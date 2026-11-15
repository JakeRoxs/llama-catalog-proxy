//go:build !linux

package modelconfig

import (
	"errors"
	"os"
)

func openTargetFile(_ *os.Root, _ string) (*os.File, error) {
	return nil, errors.New("management target access is unsupported on this platform")
}
