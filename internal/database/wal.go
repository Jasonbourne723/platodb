package database

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

type Wal struct {
	fd *os.File
}

func (w *Wal) Write(key string, value []byte) error {

	bytes, err := encode(key, value)
	if err != nil {
		return err
	}

	w.fd.Write(bytes)
	return nil
}

func (w *Wal) ReadAll() error {
	return nil
}

func encode(key string, value []byte) ([]byte, error) {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.BigEndian, byte(1)); err != nil {
		return nil, err
	}

	keyBytes := []byte(key)

	crc := crc32.ChecksumIEEE(append(keyBytes, value...))
	if err := binary.Write(&buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, byte(len(keyBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(keyBytes); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(value))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
