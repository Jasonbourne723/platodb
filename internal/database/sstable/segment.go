package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

const (
	BLOCK_SIZE    = 64 * common.KB
	FILEMODE_PERM = 0644
	SEG_SUFFIX    = ".seg"
	SP_SUFFIX     = ".sp"
)

type snapshotBlock struct {
	min string
	max string
}

type segment struct {
	id        int64
	file      *os.File
	filePath  string
	closed    int32
	blocks    []block
	snapshots []snapshotBlock
	size      int64
	utils     *common.Utils
}

// 创建一个新的segment文件
func newSegment(root string, id int64) (*segment, error) {

	name := fmt.Sprintf("%06d%s", id, SEG_SUFFIX)
	filePath := path.Join(root, name)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, FILEMODE_PERM)
	if err != nil {
		return nil, fmt.Errorf("segment文件打开失败:%w", err)
	}
	return &segment{
		id:        id,
		file:      file,
		filePath:  filePath,
		blocks:    make([]block, 0, 50),
		snapshots: make([]snapshotBlock, 0, 50),
		utils:     common.NewUtils(),
	}, nil
}

// 加载已存在的segment文件
func loadSegment(root string, name string) (*segment, error) {
	if !strings.HasSuffix(name, SEG_SUFFIX) {
		return nil, errors.New("FILE FORMAT ERROR")
	}
	id, err := strconv.Atoi(strings.Split(name, ".")[0])
	if err != nil {
		return nil, err
	}
	filePath := path.Join(root, name)

	// 获取文件信息
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filePath, os.O_RDONLY, FILEMODE_PERM)
	if err != nil {
		return nil, err
	}
	seg := &segment{
		id:       int64(id),
		filePath: filePath,
		file:     file,
		closed:   0,
		size:     fileInfo.Size(),
	}
	seg.initBlocks()

	return seg, nil
}

func (s *segment) getSnapshotFilePath() string {
	return strings.TrimSuffix(s.filePath, SEG_SUFFIX) + SP_SUFFIX
}

// 写入数据
func (s *segment) write(chunk *common.Chunk) error {
	data, err := s.utils.Encode(chunk)
	if err != nil {
		return err
	}

	l := int64(len(data))

	if l > BLOCK_SIZE {
		return errors.New("too large")
	}

	block := s.getLatestEnonghBlock(l)
	return block.addChunk(chunk, data)
}

// 查询key-value；
// deleted为true，表示数据已被删除；
// ok为true，表示查询到key-value；
func (s *segment) get(key string) (chunk *common.Chunk, err error) {

	for i := range s.blocks {
		chunk, err := s.blocks[i].get(key)
		if err != nil || chunk != nil {
			return chunk, err
		}
	}
	return nil, nil
}

func (s *segment) generateSnapshot() {

	spFilePath := s.getSnapshotFilePath()
	f, err := os.OpenFile(spFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {

	}

	buf := &bytes.Buffer{}
	blockCount := make([]byte, 4)
	binary.BigEndian.PutUint32(blockCount, uint32(len(s.blocks)))
	buf.Write(blockCount)
	buf.Write([]byte("\r\n"))
	for i := range s.blocks {
		chunks := s.blocks[i].chunks
		min := chunks[0]
		minKeyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(minKeyLen, uint32(len(min.Key)))
		buf.Write(minKeyLen)
		buf.Write([]byte("\r\n"))
		buf.Write([]byte(min.Key))
		buf.Write([]byte("\r\n"))

		max := chunks[len(chunks)-1]
		maxKeyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(maxKeyLen, uint32(len(max.Key)))
		buf.Write(maxKeyLen)
		buf.Write([]byte("\r\n"))
		buf.Write([]byte(max.Key))
		buf.Write([]byte("\r\n"))
	}

	f.Write(buf.Bytes())

}

func (s *segment) initBlocks() {

	blockCount := int(math.Ceil(float64(s.size) / float64(BLOCK_SIZE)))
	s.blocks = make([]block, 0, blockCount)

	for i := 0; i < blockCount; i++ {
		s.blocks = append(s.blocks, newBlock(s, int64(i*BLOCK_SIZE)))
	}
}

// 同步文件系统
func (s *segment) sync() error {
	return s.file.Sync()
}

// 关闭文件流
func (s *segment) close() error {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return s.file.Close()
	}
	return nil
}

// 获取最后一个块，如果最后一个块容量不足，会新建一个块
func (s *segment) getLatestEnonghBlock(l int64) *block {
	length := len(s.blocks)
	if length == 0 {
		s.blocks = append(s.blocks, newBlock(s, 0))
	} else if !s.blocks[length-1].enough(l) {
		s.file.Write(make([]byte, BLOCK_SIZE-s.blocks[length-1].size))
		s.blocks = append(s.blocks, newBlock(s, int64(length*BLOCK_SIZE)))
	}
	return &s.blocks[len(s.blocks)-1]
}
