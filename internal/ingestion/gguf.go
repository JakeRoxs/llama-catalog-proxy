package ingestion

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

const (
	maxGGUFMetadataBytes = 64 << 20
	maxGGUFMetadataKeys  = 8192
	maxGGUFArrayElements = 1 << 20
	maxGGUFKeyBytes      = 1024
	maxGGUFTextBytes     = 4096
	// generalNameKey is the GGUF metadata key for the model name.
	generalNameKey = "general.name"
)

var (
	ErrInvalidGGUF      = errors.New("invalid or truncated GGUF metadata")
	ErrUnsupportedGGUF  = errors.New("unsupported GGUF version or metadata type")
	ErrGGUFInspectLimit = errors.New("GGUF metadata exceeds inspection limits")
	ErrGGUFRead         = errors.New("GGUF metadata could not be read")
)

// GGUFMetadata describes metadata only; tensor data, integrity, and loadability
// are not verified by inspection.
type GGUFMetadata struct {
	Version      uint32  `json:"version"`
	TensorCount  uint64  `json:"tensor_count"`
	MetadataKeys uint64  `json:"metadata_keys"`
	Name         *string `json:"name,omitempty"`
	Architecture *string `json:"architecture,omitempty"`
	FileType     *uint32 `json:"file_type,omitempty"`
}

type ggufReader struct {
	reader *io.LimitedReader
}

func (g *ggufReader) read(data []byte) error {
	if int64(len(data)) > g.reader.N {
		return ErrGGUFInspectLimit
	}
	if _, err := io.ReadFull(g.reader, data); err != nil {
		return ggufReadError(err)
	}
	return nil
}

func (g *ggufReader) uint32() (uint32, error) {
	var data [4]byte
	if err := g.read(data[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data[:]), nil
}

func (g *ggufReader) uint64() (uint64, error) {
	var data [8]byte
	if err := g.read(data[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data[:]), nil
}

func (g *ggufReader) skip(size uint64) error {
	if size > uint64(g.reader.N) {
		return ErrGGUFInspectLimit
	}
	if _, err := io.CopyN(io.Discard, g.reader, int64(size)); err != nil {
		return ggufReadError(err)
	}
	return nil
}

func ggufReadError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", ErrInvalidGGUF, err)
	}
	return fmt.Errorf("%w: %w", ErrGGUFRead, err)
}

func (g *ggufReader) text(limit int) (string, error) {
	size, err := g.uint64()
	if err != nil {
		return "", err
	}
	if size > uint64(limit) || size > uint64(g.reader.N) {
		return "", ErrGGUFInspectLimit
	}
	data := make([]byte, int(size))
	if err := g.read(data); err != nil {
		return "", err
	}
	if !utf8.Valid(data) {
		return "", ErrInvalidGGUF
	}
	return string(data), nil
}

func (g *ggufReader) skipString() error {
	size, err := g.uint64()
	if err != nil {
		return err
	}
	return g.skip(size)
}

var ggufScalarWidths = [...]uint64{1, 1, 2, 2, 4, 4, 4, 1, 0, 0, 8, 8, 8}

func (g *ggufReader) skipValues(valueType uint32, count uint64) error {
	if count > maxGGUFArrayElements {
		return ErrGGUFInspectLimit
	}
	if valueType == 8 {
		for i := uint64(0); i < count; i++ {
			if err := g.skipString(); err != nil {
				return err
			}
		}
		return nil
	}
	if valueType >= uint32(len(ggufScalarWidths)) || ggufScalarWidths[valueType] == 0 {
		return ErrUnsupportedGGUF
	}
	return g.skip(count * ggufScalarWidths[valueType])
}

// readGGUFHeader validates the GGUF magic, version, tensor count, and key count,
// returning the initial metadata and the number of metadata keys to iterate.
func readGGUFHeader(r *ggufReader) (GGUFMetadata, uint64, error) {
	var magic [4]byte
	if err := r.read(magic[:]); err != nil {
		return GGUFMetadata{}, 0, err
	}
	if string(magic[:]) != "GGUF" {
		return GGUFMetadata{}, 0, ErrInvalidGGUF
	}
	version, err := r.uint32()
	if err != nil {
		return GGUFMetadata{}, 0, err
	}
	if version != 2 && version != 3 {
		return GGUFMetadata{}, 0, ErrUnsupportedGGUF
	}
	tensors, err := r.uint64()
	if err != nil {
		return GGUFMetadata{}, 0, err
	}
	keys, err := r.uint64()
	if err != nil {
		return GGUFMetadata{}, 0, err
	}
	if tensors > math.MaxInt64 {
		return GGUFMetadata{}, 0, ErrInvalidGGUF
	}
	if keys > maxGGUFMetadataKeys {
		return GGUFMetadata{}, 0, ErrGGUFInspectLimit
	}
	return GGUFMetadata{Version: version, TensorCount: tensors, MetadataKeys: keys}, keys, nil
}

// readGGUFMetadataEntry processes one metadata key/value pair from the reader.
func readGGUFMetadataEntry(r *ggufReader, result *GGUFMetadata, seen map[string]bool) error {
	key, err := r.text(maxGGUFKeyBytes)
	if err != nil {
		return err
	}
	if key == "" || seen[key] {
		return ErrInvalidGGUF
	}
	seen[key] = true
	valueType, err := r.uint32()
	if err != nil {
		return err
	}
	switch {
	case (key == generalNameKey || key == "general.architecture") && valueType == 8:
		return readGGUFTextValue(r, result, key)
	case key == "general.file_type" && valueType == 4:
		return readGGUFFileType(r, result)
	case key == generalNameKey || key == "general.architecture" || key == "general.file_type":
		return ErrInvalidGGUF
	case valueType == 9:
		return readGGUFArray(r)
	default:
		return r.skipValues(valueType, 1)
	}
}

// readGGUFTextValue reads a text value for a name or architecture key.
func readGGUFTextValue(r *ggufReader, result *GGUFMetadata, key string) error {
	value, err := r.text(maxGGUFTextBytes)
	if err != nil {
		return err
	}
	if key == generalNameKey {
		result.Name = &value
	} else {
		result.Architecture = &value
	}
	return nil
}

// readGGUFFileType reads a uint32 file_type value.
func readGGUFFileType(r *ggufReader, result *GGUFMetadata) error {
	value, err := r.uint32()
	if err != nil {
		return err
	}
	result.FileType = &value
	return nil
}

// readGGUFArray skips a type-9 array value (item type, count, and items).
func readGGUFArray(r *ggufReader) error {
	itemType, err := r.uint32()
	if err != nil {
		return err
	}
	count, err := r.uint64()
	if err != nil {
		return err
	}
	return r.skipValues(itemType, count)
}

// InspectGGUF reads bounded little-endian GGUF v2/v3 metadata without reading
// tensor descriptors or tensor data.
func InspectGGUF(input io.Reader) (GGUFMetadata, error) {
	if input == nil {
		return GGUFMetadata{}, ErrInvalidGGUF
	}
	reader := ggufReader{reader: &io.LimitedReader{R: input, N: maxGGUFMetadataBytes}}
	result, keys, err := readGGUFHeader(&reader)
	if err != nil {
		return GGUFMetadata{}, err
	}
	seen := make(map[string]bool, int(keys))
	for i := uint64(0); i < keys; i++ {
		if err := readGGUFMetadataEntry(&reader, &result, seen); err != nil {
			return GGUFMetadata{}, err
		}
	}
	return result, nil
}
