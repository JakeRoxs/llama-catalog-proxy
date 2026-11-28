//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd

package auditlog

import "os"

func lockAuditFile(*os.File) (func() error, error)  { return nil, os.ErrPermission }
func validateAuditDirectoryOwner(os.FileInfo) error { return os.ErrPermission }
func validateAuditFileOwner(os.FileInfo) error      { return os.ErrPermission }
func LocalAccountID() string                        { return "local-service-account" }
