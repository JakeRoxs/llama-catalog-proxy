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

func (g *ggufReader) text(max int) (string, error) {
	size, err := g.uint64()
	if err != nil {
		return "", err
	}
	if size > uint64(max) || size > uint64(g.reader.N) {
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

// InspectGGUF reads bounded little-endian GGUF v2/v3 metadata without reading
// tensor descriptors or tensor data.
func InspectGGUF(input io.Reader) (GGUFMetadata, error) {
	if input == nil {
		return GGUFMetadata{}, ErrInvalidGGUF
	}
	reader := ggufReader{reader: &io.LimitedReader{R: input, N: maxGGUFMetadataBytes}}
	var magic [4]byte
	if err := reader.read(magic[:]); err != nil {
		return GGUFMetadata{}, err
	}
	if string(magic[:]) != "GGUF" {
		return GGUFMetadata{}, ErrInvalidGGUF
	}
	version, err := reader.uint32()
	if err != nil {
		return GGUFMetadata{}, err
	}
	if version != 2 && version != 3 {
		return GGUFMetadata{}, ErrUnsupportedGGUF
	}
	tensors, err := reader.uint64()
	if err != nil {
		return GGUFMetadata{}, err
	}
	keys, err := reader.uint64()
	if err != nil {
		return GGUFMetadata{}, err
	}
	if tensors > math.MaxInt64 {
		return GGUFMetadata{}, ErrInvalidGGUF
	}
	if keys > maxGGUFMetadataKeys {
		return GGUFMetadata{}, ErrGGUFInspectLimit
	}
	result := GGUFMetadata{Version: version, TensorCount: tensors, MetadataKeys: keys}
	seen := make(map[string]bool, int(keys))
	for i := uint64(0); i < keys; i++ {
		key, err := reader.text(maxGGUFKeyBytes)
		if err != nil {
			return GGUFMetadata{}, err
		}
		if key == "" || seen[key] {
			return GGUFMetadata{}, ErrInvalidGGUF
		}
		seen[key] = true
		valueType, err := reader.uint32()
		if err != nil {
			return GGUFMetadata{}, err
		}
		switch {
		case (key == "general.name" || key == "general.architecture") && valueType == 8:
			value, err := reader.text(maxGGUFTextBytes)
			if err != nil {
				return GGUFMetadata{}, err
			}
			if key == "general.name" {
				result.Name = &value
			} else {
				result.Architecture = &value
			}
		case key == "general.file_type" && valueType == 4:
			value, err := reader.uint32()
			if err != nil {
				return GGUFMetadata{}, err
			}
			result.FileType = &value
		case key == "general.name" || key == "general.architecture" || key == "general.file_type":
			return GGUFMetadata{}, ErrInvalidGGUF
		case valueType == 9:
			itemType, err := reader.uint32()
			if err != nil {
				return GGUFMetadata{}, err
			}
			count, err := reader.uint64()
			if err != nil {
				return GGUFMetadata{}, err
			}
			if err := reader.skipValues(itemType, count); err != nil {
				return GGUFMetadata{}, err
			}
		default:
			if err := reader.skipValues(valueType, 1); err != nil {
				return GGUFMetadata{}, err
			}
		}
	}
	return result, nil
}
