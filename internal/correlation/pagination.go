package correlation

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
	"unicode/utf8"
)

var (
	// ErrSnapshotExpired is returned when a cursor's snapshot has expired
	// or been evicted. It is a stable error, not an empty success or live
	// fallback.
	ErrSnapshotExpired = errors.New("snapshot expired")
	// ErrSnapshotNotFound is returned when a cursor is malformed, forged,
	// unknown, or does not belong to this store.
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrSnapshotTooLarge is returned when a single snapshot would exceed
	// the per-snapshot record cap.
	ErrSnapshotTooLarge = errors.New("snapshot too large")
	// ErrSnapshotEntropy is returned when the entropy source fails and
	// a crypto-random ID or secret cannot be generated.
	ErrSnapshotEntropy = errors.New("snapshot entropy failure")
)

const (
	// maxCursorLength bounds the byte length of an opaque cursor.
	maxCursorLength = 256
	// snapshotIDSize is the byte length of the crypto-random snapshot ID.
	snapshotIDSize = 16
	// macSize is the byte length of the HMAC authentication tag.
	macSize = 32
	// cursorPayloadSize is the fixed byte length of an encoded cursor
	// payload: snapshotIDSize random bytes + 8-byte offset + macSize tag.
	cursorPayloadSize = snapshotIDSize + 8 + macSize

	// snapshotDefaultCountCap is the default maximum number of snapshots per
	// correlator store.
	snapshotDefaultCountCap = 100
	// snapshotDefaultSizeCap is the default maximum total bytes of snapshots
	// per correlator store.
	snapshotDefaultSizeCap = 10 * 1024 * 1024
	// snapshotDefaultPageCap is the default maximum number of records frozen
	// in a single snapshot.
	snapshotDefaultPageCap = 1000
	// snapshotDefaultTTL is the default time-to-live for a snapshot.
	snapshotDefaultTTL = 5 * time.Minute
	// storeSecretSize is the byte length of the per-store HMAC secret.
	storeSecretSize = 32
)

// PageRequest is a request for a single page of correlation records.
type PageRequest struct {
	// Cursor is the opaque cursor returned by a previous QueryPage call.
	// Empty for the first page of a new snapshot.
	Cursor string `json:"cursor,omitempty"`
	// Limit is the maximum number of records to return in this page.
	// When creating a new snapshot (empty cursor), this sets the page size
	// for all pages of that snapshot. When resuming (non-empty cursor), the
	// page size is fixed by the snapshot and this field is validated
	// against the stored page size.
	Limit int `json:"limit,omitempty"`
}

// PageResponse is the response for a single page of correlation records.
type PageResponse struct {
	// Records are defensive copies of the page's records. Mutating them
	// does not affect internal state or the frozen snapshot. Empty pages
	// encode as empty arrays, not null.
	Records []*CorrelationRecord `json:"records"`
	// Cursor is the opaque cursor for the next page, or empty if this is
	// the last page.
	Cursor string `json:"cursor,omitempty"`
}

// snapshotState is a frozen snapshot of filtered, sorted correlation
// records. The records are defensive copies so primary record eviction
// cannot mutate the snapshot. The filter is bound to the cursor via a SHA256
// digest of the normalized filter (never the raw filter), and the page size
// is bound so that changed filters or page sizes are rejected on resume.
type snapshotState struct {
	randomID     string
	records      []*CorrelationRecord
	pageSize     int
	storeID      string
	filterDigest []byte
	createdAt    time.Time
	sizeBytes    int64
}

// newSnapshotID returns a crypto-random snapshot ID using the given entropy
// reader. It fails closed: if the entropy source fails, an error is returned
// and no predictable fallback is used.
func newSnapshotID(reader io.Reader) (string, error) {
	buf := make([]byte, snapshotIDSize)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", ErrSnapshotEntropy
	}
	return string(buf), nil
}

// newStoreSecret returns a crypto-random per-store HMAC secret using the
// given entropy reader. It fails closed on entropy failure.
func newStoreSecret(reader io.Reader) ([]byte, error) {
	buf := make([]byte, storeSecretSize)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, ErrSnapshotEntropy
	}
	return buf, nil
}

// encodeCursor returns an opaque, authenticated cursor binding a snapshot
// random ID, page offset, and store secret. The cursor uses a fixed-width
// binary layout (16-byte ID + uint64 offset + 32-byte MAC) so that
// crypto-random IDs containing NUL bytes cannot be split apart. It is
// unforgeable: any change to the offset or random ID invalidates the HMAC.
// It contains no credentials or internal data beyond the unguessable snapshot
// ID, pagination offset, and authentication tag.
func encodeCursor(storeSecret []byte, randomID []byte, offset uint64) (string, error) {
	if len(randomID) != snapshotIDSize {
		return "", ErrSnapshotEntropy
	}
	mac := hmac.New(sha256.New, storeSecret)
	mac.Write(randomID)
	var off [8]byte
	binary.BigEndian.PutUint64(off[:], offset)
	mac.Write(off[:])
	sum := mac.Sum(nil)

	var buf [cursorPayloadSize]byte
	copy(buf[:], randomID)
	copy(buf[snapshotIDSize:], off[:])
	copy(buf[snapshotIDSize+8:], sum)
	return base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

// decodeCursor parses and authenticates an opaque cursor. It rejects
// malformed, forged, overlong, or tampered cursors. Any change to the
// offset or random ID invalidates the HMAC.
func decodeCursor(storeSecret []byte, cursor string) (randomID string, offset int, err error) {
	if cursor == "" {
		return "", 0, ErrSnapshotNotFound
	}
	if len(cursor) > maxCursorLength {
		return "", 0, ErrSnapshotNotFound
	}

	// Strict base64 decode; any alphabet or length anomaly is rejection.
	if len(cursor) != base64.RawURLEncoding.EncodedLen(cursorPayloadSize) {
		return "", 0, ErrSnapshotNotFound
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(cursor)
	if err != nil {
		return "", 0, ErrSnapshotNotFound
	}
	if len(decoded) != cursorPayloadSize {
		return "", 0, ErrSnapshotNotFound
	}

	randomID = string(decoded[:snapshotIDSize])
	offsetVal := binary.BigEndian.Uint64(decoded[snapshotIDSize : snapshotIDSize+8])
	// Bounded int conversion: offsets larger than the address space cannot
	// be used as a slice index.
	if offsetVal > uint64(math.MaxInt) {
		return "", 0, ErrSnapshotNotFound
	}
	offset = int(offsetVal)

	// Verify HMAC over randomID + offset bytes.
	mac := hmac.New(sha256.New, storeSecret)
	mac.Write(decoded[:snapshotIDSize+8])
	expectedMAC := mac.Sum(nil)
	if !hmac.Equal(expectedMAC, decoded[snapshotIDSize+8:]) {
		return "", 0, ErrSnapshotNotFound
	}

	return randomID, offset, nil
}

// filterDigest returns the SHA256 digest of a normalized filter. It validates
// the filter before hashing: string fields must be valid UTF8 and no longer
// than 256 bytes; time fields are normalized to UTC instants so equal
// instants hash identically regardless of location. The normalized encoding
// includes all filter fields, including the legacy Limit/Offset, so any change
// to the filter on resume is rejected. Raw filters are never retained.
func filterDigest(filter QueryFilter) ([]byte, error) {
	for _, s := range []string{filter.BackendID, filter.ModelID, filter.Status, filter.RequestID} {
		if s == "" {
			continue
		}
		if !utf8.ValidString(s) {
			return nil, ErrSnapshotNotFound
		}
		if len(s) > 256 {
			return nil, ErrSnapshotNotFound
		}
	}
	var buf []byte
	buf = appendTime(buf, filter.TimeStart)
	buf = appendTime(buf, filter.TimeEnd)
	buf = appendString(buf, filter.BackendID)
	buf = appendString(buf, filter.ModelID)
	buf = appendString(buf, filter.Status)
	buf = appendString(buf, filter.RequestID)
	buf = appendInt(buf, int64(filter.Limit))
	buf = appendInt(buf, int64(filter.Offset))
	h := sha256.New()
	h.Write(buf)
	return h.Sum(nil), nil
}

func appendTime(buf []byte, t time.Time) []byte {
	if t.IsZero() {
		buf = append(buf, 0)
		buf = appendInt(buf, 0)
		return appendInt(buf, 0)
	}
	buf = append(buf, 1)
	utc := t.UTC()
	buf = appendInt(buf, utc.Unix())
	buf = appendInt(buf, int64(utc.Nanosecond()))
	return buf
}

func appendString(buf []byte, s string) []byte {
	buf = appendInt(buf, int64(len(s)))
	buf = append(buf, s...)
	return buf
}

func appendInt(buf []byte, v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return append(buf, b[:]...)
}
