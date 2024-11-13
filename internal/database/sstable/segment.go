package sstable

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB = 1 << (10 * iota)
	GB = 1 << (10 * iota)

	BLOCK_SIZE   = 64 * KB
	SEGMENT_SIZE = 8 * MB
)

const (
	FILEMODE_PERM = 0644
	SUFFIX        = "seg"
)

type Segment struct {
	id       int64
	file     *os.File
	filePath string
	closed   bool
	blocks   []Block
	size     int64
	utils    *common.Utils
}

// 创建一个新的segment文件
func NewSegment(root string, id int64) (*Segment, error) {

	name := fmt.Sprintf("%06d.%s", id, SUFFIX)
	filePath := path.Join(root, name)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, FILEMODE_PERM)
	if err != nil {
		return nil, fmt.Errorf("segment文件打开失败:%w", err)
	}
	return &Segment{
		id:       id,
		file:     file,
		filePath: filePath,
		blocks:   make([]Block, 0, 50),
		utils:    common.NewUtils(),
	}, nil
}

// 加载已存在的segment文件
func LoadSegment(root string, name string) (*Segment, error) {
	if !strings.HasSuffix(name, SUFFIX) {
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
	seg := &Segment{
		id:       int64(id),
		filePath: filePath,
		file:     file,
		closed:   false,
		size:     fileInfo.Size(),
	}

	return seg, nil
}

// 写入数据
func (s *Segment) Write(chunk *common.Chunk) error {

	data, err := s.utils.Encode(chunk)
	if err != nil {
		return err
	}

	l := int64(len(data))

	if l > BLOCK_SIZE {
		return errors.New("too large")
	}

	block := s.getLatestEnonghBlock(l)
	return block.AddChunk(chunk, data)
}

// 获取最后一个块，如果最后一个块容量不足，会新建一个块
func (s *Segment) getLatestEnonghBlock(l int64) *Block {
	length := len(s.blocks)
	if length == 0 {
		s.blocks = append(s.blocks, *NewBlock(s, 0))
	} else if !s.blocks[length-1].Enough(l) {
		s.file.Write(make([]byte, BLOCK_SIZE-s.blocks[length-1].size))
		s.blocks = append(s.blocks, *NewBlock(s, int64(length*BLOCK_SIZE)))
	}
	return &s.blocks[len(s.blocks)-1]
}

// 查询key-value；
// deleted为true，表示数据已被删除；
// ok为true，表示查询到key-value；
func (s *Segment) Get(key string) (chunk *common.Chunk, err error) {

	//初始化blcoks
	if len(s.blocks) == 0 {

		blockCount := int(math.Ceil(float64(s.size) / float64(BLOCK_SIZE)))
		s.blocks = make([]Block, 0, blockCount)

		for i := 0; i < blockCount; i++ {
			s.blocks = append(s.blocks, *NewBlock(s, int64(i*BLOCK_SIZE)))
		}
	}

	for i := range s.blocks {
		chunk, err := s.blocks[i].Get(key)
		if err != nil || chunk != nil {
			return chunk, err
		}
	}
	return nil, nil
}

// 同步文件系统
func (s *Segment) Sync() error {
	return s.file.Sync()
}

// 关闭文件流
func (s *Segment) Close() error {
	return s.file.Close()
}
