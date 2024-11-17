package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

func NewUtils() *Utils {
	p := &sync.Pool{}
	p.New = func() any {
		return &bytes.Buffer{}
	}

	return &Utils{
		pool: p,
	}
}

type Utils struct {
	pool *sync.Pool
}

func (u *Utils) Encode(chunk *Chunk) ([]byte, error) {

	buf := u.pool.Get().(*bytes.Buffer)
	defer u.pool.Put(buf)

	buf.Reset()

	deleted := uint8(0)
	if chunk.Deleted {
		deleted = 1
		chunk.Value = nil
	}
	if err := buf.WriteByte(deleted); err != nil {
		return nil, err
	}

	keyBytes := []byte(chunk.Key)
	keyLen := uint8(len(keyBytes))

	crc := crc32.ChecksumIEEE(append(keyBytes, chunk.Value...))
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(keyLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}
	if !chunk.Deleted {
		valueLen := uint16(len(chunk.Value))
		if err := binary.Write(buf, binary.BigEndian, valueLen); err != nil {
			return nil, err
		}
		if _, err := buf.Write(chunk.Value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
func EnsureDirExists(dirPath string) error {
	absPath, err := filepath.Abs(dirPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		err = os.MkdirAll(absPath, 0774)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check directory: %w", err)
	}
	return nil
}
