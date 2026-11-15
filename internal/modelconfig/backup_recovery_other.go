//go:build !linux

package modelconfig

func ListBackups(string) ([]BackupSummary, error) { return nil, ErrBackupStore }

func ReadBackupInfo(string, string) (BackupInfo, error) { return BackupInfo{}, ErrBackupStore }

func DeleteBackup(string, string) error { return ErrBackupStore }
