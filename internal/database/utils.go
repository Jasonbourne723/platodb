package database

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"sync"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

func New() *utils {
	p := &sync.Pool{}
	p.New = func() any {
		return &bytes.Buffer{}
	}

	return &utils{}
}

type utils struct {
	pool *sync.Pool
}

func (u *utils) Encode(chunk *common.Chunk) ([]byte, error) {

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
