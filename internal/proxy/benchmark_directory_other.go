//go:build !linux

package proxy

import "errors"

func validateBenchmarkDirectory(string) error {
	return errors.New("benchmark web management is supported only on Linux")
}
