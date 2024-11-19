package sstable

import (
	"path"
	"strconv"
	"testing"

	"github.com/Jasonbourne723/platodb/internal/database/common"
	"github.com/stretchr/testify/assert"
)

func TestNewSegment(t *testing.T) {
	// 创建临时目录
	tempDir := "D://platodb//"

	// 创建一个新的Segment
	segment, err := newSegment(tempDir, 1)
	assert.NoError(t, err, "Failed to create new segment")
	assert.NotNil(t, segment, "Segment should not be nil")
	assert.Equal(t, int64(1), segment.id, "Segment ID should be 1")
	assert.Equal(t, path.Join(tempDir, "000001.seg"), segment.filePath, "Segment file path is incorrect")
	assert.NotNil(t, segment.file, "Segment file should be opened")
}

func TestWriteAndGet(t *testing.T) {
	// 创建临时目录
	tempDir := "D://platodb//"

	// 创建一个新的Segment
	segment, err := newSegment(tempDir, 1)
	assert.NoError(t, err, "Failed to create new segment")

	// 创建一个chunk并写入Segment
	chunk := &common.Chunk{
		Key:     "key1",
		Value:   []byte("value1"),
		Deleted: false,
	}
	err = segment.write(chunk)
	assert.NoError(t, err, "Failed to write chunk to segment")

	// 获取写入的chunk
	result, err := segment.get("key1")
	assert.NoError(t, err, "Failed to get chunk from segment")
	assert.NotNil(t, result, "Result chunk should not be nil")
	assert.Equal(t, "value1", string(result.Value), "Chunk value should be 'value1'")

	// 获取不存在的key
	result, err = segment.get("nonexistent_key")
	assert.NoError(t, err, "Failed to get chunk for nonexistent key")
	assert.Nil(t, result, "Result chunk should be nil for nonexistent key")
}

func TestSync(t *testing.T) {
	// 创建临时目录
	tempDir := "D://platodb//"
	// 创建一个新的Segment
	segment, err := newSegment(tempDir, 1)
	assert.NoError(t, err, "Failed to create new segment")

	// 同步文件
	err = segment.sync()
	assert.NoError(t, err, "Failed to sync segment file")
}

func TestClose(t *testing.T) {
	// 创建临时目录
	tempDir := "D://platodb//"

	// 创建一个新的Segment
	segment, err := newSegment(tempDir, 1)
	assert.NoError(t, err, "Failed to create new segment")

	// 关闭文件
	err = segment.close()
	assert.NoError(t, err, "Failed to close segment file")

	// 再次尝试关闭，应不再报错
	err = segment.close()
	assert.NoError(t, err, "Closing again should not return error")
}

func TestWriteToNewBlock(t *testing.T) {
	// 创建临时目录
	tempDir := "D://platodb//"

	// 创建一个新的Segment
	segment, err := newSegment(tempDir, 1)
	assert.NoError(t, err, "Failed to create new segment")

	// 写入多个chunk，确保写入新块
	for i := 0; i < 5000; i++ {
		chunk := &common.Chunk{
			Key:     "key" + strconv.Itoa(i),
			Value:   []byte("value" + strconv.Itoa(i)),
			Deleted: false,
		}
		err = segment.write(chunk)
		assert.NoError(t, err, "Failed to write chunk to segment")
	}

	// 检查Segment的块数
	assert.Greater(t, len(segment.blocks), 1, "There should be more than 1 block")
}

func TestLoadSegment(t *testing.T) {
	// 创建临时目录
	tempDir := "D://platodb//"

	// 创建一个新的Segment并写入
	segment, err := newSegment(tempDir, 1)
	assert.NoError(t, err, "Failed to create new segment")
	chunk := &common.Chunk{
		Key:     "key1",
		Value:   []byte("value1"),
		Deleted: false,
	}
	err = segment.write(chunk)
	assert.NoError(t, err, "Failed to write chunk to segment")

	// 关闭并重新加载Segment
	err = segment.close()
	assert.NoError(t, err, "Failed to close segment")

	// 加载该segment
	loadedSegment, err := loadSegment(tempDir, "000001.seg")
	assert.NoError(t, err, "Failed to load segment")
	assert.Equal(t, segment.id, loadedSegment.id, "Loaded segment ID should match")
}
