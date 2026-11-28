//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !windows

package federation

import "os"

func lockPairingStore(*os.File) (func() error, error) { return nil, os.ErrPermission }
func validatePairingStoreOwner(os.FileInfo) error     { return os.ErrPermission }
