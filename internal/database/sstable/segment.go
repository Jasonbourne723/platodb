package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB = 1 << (10 * iota)
	GB = 1 << (10 * iota)

	BLOCK_SIZE   = 32 * KB
	SEGMENT_SIZE = 256 * MB
)

type Segment struct {
	file             *os.File
	writer           *bufio.Writer
	currentBlockNum  int32
	currentBlockSize int32
	buf              *bytes.Buffer
}

func NewSegment(path string) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("seg文件打开失败:%w", err)
	}
	return &Segment{
		file:   file,
		writer: bufio.NewWriter(file),
		buf:    &bytes.Buffer{},
	}, nil
}

func (s *Segment) Write(key string, value []byte) error {

	data, err := s.Encode(key, value)
	if err != nil {
		return err
	}

	l := int32(len(data))
	if s.currentBlockSize+l > BLOCK_SIZE {
		s.currentBlockNum++
		s.currentBlockSize = 0
	}
	defer func() {
		s.currentBlockSize += l
	}()
	_, err = s.writer.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Segment) Sync() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *Segment) Close() error {
	if err := s.writer.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}

func (s *Segment) Encode(key string, value []byte) ([]byte, error) {
	s.buf.Reset()

	if err := s.buf.WriteByte(1); err != nil {
		return nil, err
	}

	keyBytes := []byte(key)
	keyLen := uint8(len(keyBytes))
	valueLen := uint16(len(value))

	crc := crc32.ChecksumIEEE(append(keyBytes, value...))
	if err := binary.Write(s.buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}
	if err := s.buf.WriteByte(keyLen); err != nil {
		return nil, err
	}
	if _, err := s.buf.Write(keyBytes); err != nil {
		return nil, err
	}
	if err := binary.Write(s.buf, binary.BigEndian, valueLen); err != nil {
		return nil, err
	}
	if _, err := s.buf.Write(value); err != nil {
		return nil, err
	}
	return s.buf.Bytes(), nil
}
