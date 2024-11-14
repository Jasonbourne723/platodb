package sstable

import (
	"os"
	"testing"

	"github.com/Jasonbourne723/platodb/internal/database/common"
	"github.com/stretchr/testify/assert"
)

func TestBlock_AddChunk(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_segment_")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer tempFile.Close()

	seg := &Segment{file: tempFile}
	block := NewBlock(seg, 0)

	chunk := &common.Chunk{
		Key:     "key1",
		Value:   []byte("value1"),
		Deleted: false,
	}
	data := append([]byte{0}, []byte("key1")...) // 模拟存储的数据

	// 添加数据块
	err = block.AddChunk(chunk, data)
	assert.NoError(t, err)
	assert.Equal(t, int64(len(data)), block.size)
}

func TestBlock_Get(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_segment_")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer tempFile.Close()

	seg := &Segment{file: tempFile}
	block := NewBlock(seg, 0)

	// 插入一个数据块
	chunk := &common.Chunk{
		Key:     "key1",
		Value:   []byte("value1"),
		Deleted: false,
	}
	data := append([]byte{0}, []byte("key1")...)
	block.AddChunk(chunk, data)

	// 从块中获取数据
	retrievedChunk, err := block.Get("key1")
	assert.NoError(t, err)
	assert.NotNil(t, retrievedChunk)
	assert.Equal(t, "key1", retrievedChunk.Key)
	assert.Equal(t, []byte("value1"), retrievedChunk.Value)
}

func TestBlock_LoadDataFromDisk(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_segment_")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer tempFile.Close()
	// 获取文件信息
	// 获取临时文件的信息
	fileInfo, err := tempFile.Stat()
	if err != nil {
		t.Fatalf("Unable to get file info: %v", err)
	}
	seg := &Segment{file: tempFile, size: fileInfo.Size()}
	block := NewBlock(seg, 0)

	// 模拟写入数据
	chunk := &common.Chunk{
		Key:     "key1",
		Value:   []byte("value1"),
		Deleted: false,
	}
	data, _ := common.NewUtils().Encode(chunk)
	block.AddChunk(chunk, data)

	// 模拟从磁盘读取数据
	// 需要重新打开文件并读取
	seg.file.Seek(0, 0)
	block2 := NewBlock(seg, 0)
	err = block2.LoadDataFromDisk()
	assert.NoError(t, err)

	// 验证加载的数据
	retrievedChunk, err := block2.Get("key1")
	assert.NoError(t, err)
	assert.NotNil(t, retrievedChunk)
	assert.Equal(t, "key1", retrievedChunk.Key)
	assert.Equal(t, []byte("value1"), retrievedChunk.Value)
}

func TestBlock_MiddleSearch(t *testing.T) {
	// 创建临时文件
	tempFile, err := os.CreateTemp("", "test_segment_")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer tempFile.Close()

	seg := &Segment{file: tempFile}
	block := NewBlock(seg, 0)

	// 插入数据
	chunks := []common.Chunk{
		{Key: "key1", Value: []byte("value1"), Deleted: false},
		{Key: "key2", Value: []byte("value2"), Deleted: false},
		{Key: "key3", Value: []byte("value3"), Deleted: false},
	}
	for _, chunk := range chunks {
		data := append([]byte{0}, []byte(chunk.Key)...) // 模拟存储的数据
		block.AddChunk(&chunk, data)
	}

	// 查找已存在的 key
	chunk, ok := block.MiddleSearch("key2", 0, int64(len(block.chunks))-1)
	assert.True(t, ok)
	assert.Equal(t, "key2", chunk.Key)

	// 查找不存在的 key
	chunk, ok = block.MiddleSearch("key4", 0, int64(len(block.chunks))-1)
	assert.False(t, ok)
	assert.Nil(t, chunk)
}
