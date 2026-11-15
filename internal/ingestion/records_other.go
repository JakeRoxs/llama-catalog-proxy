//go:build !linux

package ingestion

func openRecordStore(string, int) (recordStore, error) {
	return nil, ErrRecordUnsupported
}
