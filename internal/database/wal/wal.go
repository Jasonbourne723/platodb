package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path"
	"time"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

const (
	ROOT   = "D://platodb//wal/"
	SUFFIX = ".log"
)

type WalWriterCloser interface {
	WalWriter
	WalCloser
}

type WalReaderCloser interface {
	WalReader
	WalCloser
}

type WalReader interface {
	Read() (*common.Chunk, error)
}

type WalWriter interface {
	Write(*common.Chunk) error
}

type WalCloser interface {
	Close() error
}

type Wal struct {
	file     *os.File
	filePath string
	utils    *common.Utils
	reader   *bufio.Reader
}

func NewWalReaderCloser(filepath string) (WalReaderCloser, error) {

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

func NewWalWriterCloser() (WalWriterCloser, error) {
	filePath := path.Join(ROOT, time.Now().Format("20060102150405")+SUFFIX)
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

func (w *Wal) Sync() error {
	return w.file.Sync()
}

func (w *Wal) Close() error {
	w.file.Sync()
	w.file.Close()
	return os.Remove(w.filePath)
}
