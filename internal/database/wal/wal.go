package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"time"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

const (
	SUFFIX = ".log"
)

type WriterCloser interface {
	Writer
	Closer
}

type ReaderCloser interface {
	Reader
	Closer
}

type Reader interface {
	Read() (*common.Chunk, error)
}

type Writer interface {
	Write(*common.Chunk) error
}

type Closer interface {
	Close() error
}

type Wal struct {
	file     *os.File
	filePath string
	utils    *common.Utils
	reader   *bufio.Reader
}

// NewReaderCloser creates and returns a new WalReaderCloser instance initialized with the provided file path.
// It opens the file in read-write mode and wraps it into a Wal struct which implements ReaderCloser interface.
// If the file cannot be opened, an error is returned.
// The WalReaderCloser allows reading from and closing the Write-Ahead Log (WAL) file.
func NewReaderCloser(filepath string) (ReaderCloser, error) {

	walFile, err := os.OpenFile(filepath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &Wal{
		file:     walFile,
		filePath: filepath,
		utils:    common.NewUtils(),
		reader:   bufio.NewReader(walFile),
	}, nil
}

// NewWriterCloser creates and returns a new WriterCloser instance initialized with a Write-Ahead Log (WAL) file located in the specified directory.
// The filename is generated based on the current time and appended with a predefined suffix.
// It opens the file for writing, creating it if necessary, and wraps it within a Wal structure.
// Returns a WriterCloser interface and an error if the file operation fails.
func NewWriterCloser(walDir string) (WriterCloser, error) {
	filePath := path.Join(walDir, time.Now().Format("20060102150405")+SUFFIX)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	wal := &Wal{
		file:     file,
		filePath: filePath,
		utils:    common.NewUtils(),
	}
	return wal, nil
}

// Read decodes the next chunk from the Write-Ahead Log (WAL), verifying its integrity using CRC checksum.
// It returns a pointer to a Chunk which includes the key-value pair, and a boolean indicating deletion.
// If the chunk is marked as deleted, the value will be nil.
// It returns an error if any read operation fails or if the checksum does not match.
func (w *Wal) Read() (*common.Chunk, error) {

	deletedByte, err := w.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	deleted := deletedByte == 1

	// 读取 CRC 校验
	crcBytes := make([]byte, 4)
	_, err = io.ReadFull(w.reader, crcBytes)
	if err != nil {
		return nil, err
	}
	crc := binary.BigEndian.Uint32(crcBytes)

	// 读取 key 的长度
	keyLenByte, err := w.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	keyLen := int(keyLenByte)

	// 读取 key
	keyBytes := make([]byte, keyLen)
	_, err = io.ReadFull(w.reader, keyBytes)
	if err != nil {
		return nil, err
	}
	key := string(keyBytes)

	// 读取 value 的长度并读取 value
	var value []byte
	if !deleted {
		valueLenBytes := make([]byte, 2)
		_, err = io.ReadFull(w.reader, valueLenBytes)
		if err != nil {
			return nil, err
		}
		valueLen := int(binary.BigEndian.Uint16(valueLenBytes))

		// 读取 value
		value = make([]byte, valueLen)
		_, err = io.ReadFull(w.reader, value)
		if err != nil {
			return nil, err
		}
	}

	// 验证 CRC
	if crc != crc32.ChecksumIEEE(append(keyBytes, value...)) {
		return nil, errors.New("crc check failed")
	}

	// 将解码后的数据添加到 chunks
	return &common.Chunk{
		Key:     key,
		Value:   value,
		Deleted: deleted,
	}, nil
}

// Write encodes the provided chunk using the utility encoder, then writes the encoded bytes to the WAL file.
// Returns an error if encoding fails or writing to the file encounters an issue.
func (w *Wal) Write(chunk *common.Chunk) error {

	bytes, err := w.utils.Encode(chunk)
	if err != nil {
		return err
	}
	_, err = w.file.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// Sync ensures that any buffered data in the Write-Ahead Log (WAL) is written to the disk and flushed.
// It returns an error if the synchronization operation fails.
func (w *Wal) Sync() error {
	return w.file.Sync()
}

// Close synchronously flushes any unwritten data to disk, closes the WAL file, and removes the file from the filesystem.
// Returns an error if any of these operations fail.
func (w *Wal) Close() error {
	var err error
	if err = w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL file: %w", err)
	}
	if err = w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}
	if err = os.Remove(w.filePath); err != nil {
		return fmt.Errorf("failed to remove WAL file: %w", err)
	}
	return nil
}
