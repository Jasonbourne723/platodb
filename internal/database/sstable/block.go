package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

type block struct {
	seg      *segment
	posBegin int64
	chunks   []common.Chunk
	size     int64
	min      string
	max      string
}

func (b *block) enough(size int64) bool {
	return b.size+size <= BLOCK_SIZE
}

func newBlock(seg *segment, pos int64) block {
	return block{
		seg:      seg,
		chunks:   make([]common.Chunk, 0, 100),
		size:     0,
		posBegin: pos,
	}
}

func (b *block) addChunk(chunk *common.Chunk, data []byte) error {

	b.chunks = append(b.chunks, *chunk)
	b.size += int64(len(data))

	_, err := b.seg.file.Write(data)
	return err
}

func (b *block) get(key string) (*common.Chunk, error) {

	if len(b.chunks) == 0 { //块数据还未加载到内存
		if err := b.loadDataFromDisk(); err != nil {
			return nil, err
		}
	}

	chunk, ok := b.middleSearch(key, 0, int64(len(b.chunks))-1)
	if ok {
		return chunk, nil
	}
	return nil, nil
}

func (b *block) loadDataFromDisk() error {
	buf := make([]byte, BLOCK_SIZE)
	n, err := b.seg.file.ReadAt(buf, b.posBegin)
	if err != nil {
		if err != io.EOF {
			return err
		}
		if n == 0 {
			return nil
		}
	}
	buf = buf[:n]
	pos := 0

	for pos < len(buf) {
		// 检查剩余字节是否足够读取特定数据
		checkRemaining := func(required int) bool {
			return pos+required <= len(buf)
		}

		// 读取墓碑标志
		if !checkRemaining(1) {
			break
		}
		deleted := buf[pos]
		pos++

		// 读取 CRC 校验
		if !checkRemaining(4) {
			break
		}
		crc := binary.BigEndian.Uint32(buf[pos : pos+4])
		if crc == 0 {
			break
		}
		pos += 4

		// 获取 key 的长度并读取 key
		if !checkRemaining(1) {
			break
		}
		keyLen := buf[pos]
		if keyLen == 0 {
			break
		}
		pos++

		if !checkRemaining(int(keyLen)) {
			break
		}
		key := string(buf[pos : pos+int(keyLen)])
		pos += int(keyLen)

		// 如果未删除，则读取 value
		var value []byte
		if deleted == 0 {
			// 读取 value 的长度
			if !checkRemaining(2) {
				break
			}
			valueLen := binary.BigEndian.Uint16(buf[pos : pos+2])
			pos += 2

			// 读取 value 数据
			if !checkRemaining(int(valueLen)) {
				break
			}
			value = buf[pos : pos+int(valueLen)]
			pos += int(valueLen)
		}
		// 校验 CRC
		if crc != crc32.ChecksumIEEE(append([]byte(key), value...)) {
			return errors.New("crc check failed")
		}

		// 将解码后的数据添加到 chunks
		b.chunks = append(b.chunks, common.Chunk{
			Key:     key,
			Value:   value,
			Deleted: deleted == 1,
		})
	}
	if len(b.chunks) > 0 {
		b.min = b.chunks[0].Key
		b.max = b.chunks[len(b.chunks)-1].Key
	}
	return nil
}

// 二分法 快速查询
func (b *block) middleSearch(key string, posBegin int64, posEnd int64) (c *common.Chunk, ok bool) {

	if key < b.chunks[posBegin].Key || key > b.chunks[posEnd].Key {
		return nil, false
	}
	if key == b.chunks[posBegin].Key {
		return &b.chunks[posBegin], true
	}
	if key == b.chunks[posEnd].Key {
		return &b.chunks[posEnd], true
	}
	if posEnd == posBegin || posEnd == posBegin+1 {
		return nil, false
	}
	posMiddle := (posBegin + posEnd) / 2
	if key == b.chunks[posMiddle].Key {
		return &b.chunks[posMiddle], true
	}
	if key < b.chunks[posMiddle].Key {
		return b.middleSearch(key, posBegin, posMiddle)
	}
	return b.middleSearch(key, posMiddle, posEnd)
}
