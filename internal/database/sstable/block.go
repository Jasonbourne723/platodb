package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

type Block struct {
	seg      *Segment
	posBegin int64
	chunks   []Chunk
	size     int64
}

type Chunk struct {
	key     string
	value   []byte
	deleted bool
}

func (b *Block) Enough(size int64) bool {
	return b.size+size <= BLOCK_SIZE
}

func NewBlock(seg *Segment, pos int64) *Block {
	return &Block{
		seg:      seg,
		chunks:   make([]Chunk, 0, 100),
		size:     0,
		posBegin: pos,
	}
}

func (b *Block) AddChunk(chunk *Chunk, data []byte) error {

	b.chunks = append(b.chunks, *chunk)
	b.size += int64(len(data))

	_, err := b.seg.writer.Write(data)
	return err
}

func (b *Block) Get(key string) (*Chunk, error) {

	if len(b.chunks) == 0 { //块数据还未加载到内存
		if err := b.LoadDataFromDisk(); err != nil {
			return nil, err
		}
	}

	chunk, ok := b.MiddleSearch(key, 0, int64(len(b.chunks)))
	if ok {
		return chunk, nil
	}
	return nil, nil
}

func (b *Block) LoadDataFromDisk() error {

	buf := make([]byte, BLOCK_SIZE)
	n, err := b.seg.file.ReadAt(buf, b.posBegin)
	if err != nil {
		if err != io.EOF {
			return err
		}
		return nil
	}
	buf = buf[:n]
	pos := 0

	for {
		// 读取墓碑标志
		deleted := buf[pos]
		pos++

		//CRC 校验部分（若需要可加）
		crc := binary.BigEndian.Uint32(buf[pos : pos+4])
		pos += 4

		// 获取 key 的长度并获取 key
		keylen := buf[pos]
		pos++
		key := string(buf[pos : pos+int(keylen)])
		pos += int(keylen)

		var value []byte

		if deleted == 0 {
			valueLen := binary.BigEndian.Uint16(buf[pos : pos+2])
			pos += 2
			// 获取 value 数据
			value = buf[pos : pos+int(valueLen)]
		}

		if crc != crc32.ChecksumIEEE(append([]byte(key), value...)) {
			return errors.New("crc check failed")
		}

		b.chunks = append(b.chunks, Chunk{
			key:     key,
			value:   value,
			deleted: deleted == 1,
		})
	}
}

// 二分法 快速查询
func (b *Block) MiddleSearch(key string, posBegin int64, posEnd int64) (c *Chunk, ok bool) {

	if key < b.chunks[posBegin].key || key > b.chunks[posEnd].key {
		return nil, false
	}
	if key == b.chunks[posBegin].key {
		return &b.chunks[posBegin], true
	}
	if key == b.chunks[posEnd].key {
		return &b.chunks[posEnd], true
	}
	if posEnd == posBegin || posEnd == posBegin+1 {
		return nil, false
	}
	posMiddle := (posBegin + posEnd) / 2
	if key == b.chunks[posMiddle].key {
		return &b.chunks[posMiddle], true
	}
	if key < b.chunks[posMiddle].key {
		return b.MiddleSearch(key, posBegin, posMiddle)
	}
	return b.MiddleSearch(key, posMiddle, posEnd)
}
