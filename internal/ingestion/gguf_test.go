package ingestion

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

func ggufHeader(version uint32, tensors, keys uint64) []byte {
	data := append([]byte{}, "GGUF"...)
	data = binary.LittleEndian.AppendUint32(data, version)
	data = binary.LittleEndian.AppendUint64(data, tensors)
	return binary.LittleEndian.AppendUint64(data, keys)
}

func ggufString(data []byte, value string) []byte {
	data = binary.LittleEndian.AppendUint64(data, uint64(len(value)))
	return append(data, value...)
}

func ggufKey(data []byte, key string, valueType uint32) []byte {
	data = ggufString(data, key)
	return binary.LittleEndian.AppendUint32(data, valueType)
}

func TestInspectGGUFMetadata(t *testing.T) {
	data := ggufHeader(3, 42, 4)
	data = ggufString(ggufKey(data, "general.name", 8), "Test Model")
	data = ggufString(ggufKey(data, "general.architecture", 8), "llama")
	data = binary.LittleEndian.AppendUint32(ggufKey(data, "general.file_type", 4), 0)
	data = ggufKey(data, "tokenizer.ggml.tokens", 9)
	data = binary.LittleEndian.AppendUint32(data, 8)
	data = binary.LittleEndian.AppendUint64(data, 2)
	data = ggufString(data, "token")
	data = ggufString(data, string([]byte{0xff}))

	got, err := InspectGGUF(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 3 || got.TensorCount != 42 || got.MetadataKeys != 4 ||
		got.Name == nil || *got.Name != "Test Model" ||
		got.Architecture == nil || *got.Architecture != "llama" ||
		got.FileType == nil || *got.FileType != 0 {
		t.Fatalf("metadata = %+v", got)
	}
}

func TestInspectGGUFEmptyMetadataAndVersion2(t *testing.T) {
	got, err := InspectGGUF(bytes.NewReader(ggufHeader(2, 0, 0)))
	if err != nil || got.Version != 2 || got.Name != nil || got.Architecture != nil || got.FileType != nil {
		t.Fatalf("metadata = %+v, %v", got, err)
	}
}

func TestInspectGGUFRejectsMalformedAndBoundedMetadata(t *testing.T) {
	duplicate := ggufHeader(3, 0, 2)
	duplicate = ggufString(ggufKey(duplicate, "general.name", 8), "name")
	duplicate = ggufString(ggufKey(duplicate, "general.name", 8), "other")
	invalidText := ggufString(ggufKey(ggufHeader(3, 0, 1), "general.name", 8), string([]byte{0xff}))
	wrongType := binary.LittleEndian.AppendUint32(ggufKey(ggufHeader(3, 0, 1), "general.name", 4), 1)
	arrayLimit := ggufKey(ggufHeader(3, 0, 1), "large", 9)
	arrayLimit = binary.LittleEndian.AppendUint32(arrayLimit, 4)
	arrayLimit = binary.LittleEndian.AppendUint64(arrayLimit, maxGGUFArrayElements+1)
	stringLimit := ggufKey(ggufHeader(3, 0, 1), "other", 8)
	stringLimit = binary.LittleEndian.AppendUint64(stringLimit, maxGGUFMetadataBytes)
	nestedArray := ggufKey(ggufHeader(3, 0, 1), "nested", 9)
	nestedArray = binary.LittleEndian.AppendUint32(nestedArray, 9)
	nestedArray = binary.LittleEndian.AppendUint64(nestedArray, 0)
	for _, test := range []struct {
		name string
		data []byte
		want error
	}{
		{"wrong magic", []byte("NOPE"), ErrInvalidGGUF},
		{"short header", ggufHeader(3, 0, 0)[:12], ErrInvalidGGUF},
		{"unsupported version", ggufHeader(4, 0, 0), ErrUnsupportedGGUF},
		{"negative tensor count", ggufHeader(3, math.MaxUint64, 0), ErrInvalidGGUF},
		{"too many keys", ggufHeader(3, 0, maxGGUFMetadataKeys+1), ErrGGUFInspectLimit},
		{"duplicate keys", duplicate, ErrInvalidGGUF},
		{"invalid selected text", invalidText, ErrInvalidGGUF},
		{"wrong selected type", wrongType, ErrInvalidGGUF},
		{"array limit", arrayLimit, ErrGGUFInspectLimit},
		{"byte budget", stringLimit, ErrGGUFInspectLimit},
		{"nested array", nestedArray, ErrUnsupportedGGUF},
		{"truncated value", ggufKey(ggufHeader(3, 0, 1), "other", 10), ErrInvalidGGUF},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := InspectGGUF(bytes.NewReader(test.data)); !errors.Is(err, test.want) {
				t.Fatalf("got %v; want %v", err, test.want)
			}
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(data []byte) (int, error) {
	clear(data)
	return len(data), nil
}

func TestInspectGGUFMetadataByteBudget(t *testing.T) {
	prefix := ggufKey(ggufHeader(3, 0, 1), "ignored", 8)
	maxValue := maxGGUFMetadataBytes - len(prefix) - 8
	atLimit := binary.LittleEndian.AppendUint64(prefix, uint64(maxValue))
	if _, err := InspectGGUF(io.MultiReader(bytes.NewReader(atLimit), io.LimitReader(zeroReader{}, int64(maxValue)))); err != nil {
		t.Fatalf("metadata exactly at limit: %v", err)
	}
	overLimit := binary.LittleEndian.AppendUint64(prefix, uint64(maxValue+1))
	if _, err := InspectGGUF(io.MultiReader(bytes.NewReader(overLimit), zeroReader{})); !errors.Is(err, ErrGGUFInspectLimit) {
		t.Fatalf("metadata above limit: %v", err)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestInspectGGUFReportsReadFailure(t *testing.T) {
	diskErr := errors.New("disk failure")
	_, err := InspectGGUF(errorReader{err: diskErr})
	if !errors.Is(err, ErrGGUFRead) || !errors.Is(err, diskErr) || errors.Is(err, ErrInvalidGGUF) {
		t.Fatalf("read error = %v", err)
	}
}
