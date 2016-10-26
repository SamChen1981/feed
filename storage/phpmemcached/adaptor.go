package phpmemcached

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"

	"gitlab.meitu.com/platform/gocommons/fastlz"
	"feed/storage"
)

// Compression is the type of compression to apply when writing data to Google Cloud Storage.
type Compression string

const (
	None   Compression = "NONE"
	Zlib   Compression = "ZLIB"
	FastLZ Compression = "FASTLZ"
)

var defaultOptions *Options = &Options{
	compressionThreshold: 2000,
	compressionType:      FastLZ,
	compressionFactor:    1.3,
}

type Options struct {
	compressionThreshold int
	compressionFactor    float32
	compressionType      Compression
}

func (pmo *Options) SetCompressionType(compression Compression) *Options {
	pmo.compressionType = compression
	return pmo
}

func (pmo *Options) SetCompressionFactor(factor float32) *Options {
	if factor >= 1 && pmo.compressionType != None {
		pmo.compressionFactor = factor
	}
	return pmo
}

func (pmo *Options) SetCompressionThreshold(threshold int) *Options {
	if threshold > 0 && pmo.compressionType != None {
		pmo.compressionThreshold = threshold
	}
	return pmo
}

func (pmo *Options) String() string {
	return fmt.Sprintf(
		"Compression Type: %v, Compression Threshold: %d bytes, Compression Factor: %.2f",
		pmo.compressionType,
		pmo.compressionThreshold,
		pmo.compressionFactor,
	)
}

func NewOptions() *Options {
	return &Options{
		compressionThreshold: defaultOptions.compressionThreshold,
		compressionType:      defaultOptions.compressionType,
		compressionFactor:    defaultOptions.compressionFactor,
	}
}

type ItemAdaptor struct {
	options *Options
}

func NewItemAdaptor(options *Options) storage.ItemAdaptor {
	if options == nil {
		options = defaultOptions
	}
	return &ItemAdaptor{
		options: options,
	}
}

func (ia *ItemAdaptor) AdaptGetItem(item *storage.Item) (*storage.Item, error) {
	compressed, compression := ia.isCompressed(item.Flags)
	if !compressed {
		return item, nil
	}

	var originalSize uint32
	compressedValue := item.Value.([]byte)
	err := binary.Read(bytes.NewReader(compressedValue), binary.LittleEndian, &originalSize)
	if err != nil {
		return nil, err
	}
	decompressedValue := make([]byte, originalSize)
	switch compression {
	case FastLZ:
		l, err := fastlz.Decompress(compressedValue, 4, uint32(len(compressedValue)-4), decompressedValue, 0, originalSize)
		if err != nil {
			return nil, err
		}
		if l != originalSize {
			return nil, fmt.Errorf("fastlz decompressed length mismatch, expected: %d, got: %d", originalSize, l)
		}
		item.Value = decompressedValue
		item.Flags = DeleteFlag(item.Flags, CompressionFastLZ)
	case Zlib:
		compressedValue = compressedValue[4:]
		buf := bytes.NewBuffer(decompressedValue)
		buf.Truncate(0)
		zlibReader, err := zlib.NewReader(bytes.NewReader(compressedValue))
		defer zlibReader.Close()
		if err != nil {
			return nil, err
		}
		l, err := io.Copy(buf, zlibReader)
		if err != nil {
			return nil, err
		}
		if uint32(l) != originalSize {
			return nil, fmt.Errorf("zlib decompressed length mismatch, expected: %d, got: %d", originalSize, l)
		}
		item.Value = buf.Bytes()
		item.Flags = DeleteFlag(item.Flags, CompressionZlib)
	}

	item.Flags = DeleteFlag(item.Flags, Compressed)
	return item, nil
}

func (ia *ItemAdaptor) isCompressed(flag uint32) (bool, Compression) {
	if HasFlag(flag, Compressed) {
		if HasFlag(flag, CompressionFastLZ) {
			return true, FastLZ
		}
		if HasFlag(flag, CompressionZlib) {
			return true, Zlib
		}
	}
	return false, None
}

func (ia *ItemAdaptor) AdaptSetItem(item *storage.Item) (*storage.Item, error) {
	if ia.options.compressionType == None {
		return item, nil
	}

	isCompressed, _ := ia.isCompressed(item.Flags)
	if isCompressed {
		return item, nil
	}

	input, ok := item.Value.([]byte)
	if !ok || len(input) <= ia.options.compressionThreshold {
		return item, nil
	}

	inputLength := uint32(len(input))
	switch ia.options.compressionType {
	case FastLZ:
		outputBufferSize := fastlz.CalculateOutputBufferLength(inputLength)
		outputBuffer := make([]byte, 4+outputBufferSize)
		binary.LittleEndian.PutUint32(outputBuffer, inputLength)
		cl, err := fastlz.Compress(input, 0, inputLength, outputBuffer, 4, fastlz.LevelAuto)
		if err != nil {
			return item, err
		}
		if float32(inputLength) > float32(cl)*ia.options.compressionFactor {
			flags := SetFlag(item.Flags, Compressed)
			flags = SetFlag(flags, CompressionFastLZ)
			return &storage.Item{
				Value:       outputBuffer[:4+cl],
				Flags:       flags,
				DataVersion: item.DataVersion,
				ExpireAt:    item.ExpireAt,
			}, nil
		}

	case Zlib:
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, inputLength)
		w := zlib.NewWriter(&buf)
		_, err := w.Write(input)
		if err != nil {
			return item, err
		}
		err = w.Flush()
		if err != nil {
			return item, err
		}
		err = w.Close()
		if err != nil {
			return item, err
		}

		if float32(inputLength) > float32(buf.Len()-4)*ia.options.compressionFactor {
			flags := SetFlag(item.Flags, Compressed)
			flags = SetFlag(flags, CompressionZlib)
			return &storage.Item{
				Value:       buf.Bytes(),
				Flags:       flags,
				DataVersion: item.DataVersion,
				ExpireAt:    item.ExpireAt,
			}, nil
		}
	}

	return item, nil
}
