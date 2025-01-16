package sstable

import (
	"context"
	"path"
	"path/filepath"
	"testing"

	"github.com/Jasonbourne723/platodb/internal/database/common"
	"github.com/stretchr/testify/assert"
)

// Mock Scanner 实现
type MockScanner struct {
	data     []common.Chunk
	position int
}

func (m *MockScanner) Scan() bool {
	if m.position < len(m.data) {
		m.position++
		return true
	}
	return false
}

func (m *MockScanner) ScanValue() *common.Chunk {
	if m.position > 0 {
		return &m.data[m.position-1]
	}
	return nil
}

func TestSSTable(t *testing.T) {
	// Setup
	tempDir := "D://platodb//"

	// 创建 SSTable
	sstable, err := NewSSTable(tempDir, context.Background())
	assert.NoError(t, err, "Failed to create SSTable")
	assert.NotNil(t, sstable, "SSTable should not be nil")

	// 模拟一些数据写入
	chunks := []common.Chunk{
		{Key: "key1", Value: []byte("value1"), Deleted: false},
		{Key: "key2", Value: []byte("value2"), Deleted: false},
		{Key: "key3", Value: []byte("value3"), Deleted: false},
	}
	scanner := &MockScanner{data: chunks}

	// 写入数据到 SSTable
	err = sstable.Write(scanner)
	assert.NoError(t, err, "Failed to write data to SSTable")

	// 获取 key
	value, err := sstable.Get("key1")
	assert.NoError(t, err, "Failed to get value for key1")
	assert.Equal(t, []byte("value1"), value, "Value for key1 should match")

	// 获取不存在的 key
	value, err = sstable.Get("key_nonexistent")
	assert.NoError(t, err, "Failed to get value for non-existent key")
	assert.Nil(t, value, "Value for non-existent key should be nil")

	// 模拟删除数据
	chunks[1].Deleted = true
	scanner = &MockScanner{data: chunks}
	err = sstable.Write(scanner)
	assert.NoError(t, err, "Failed to write deleted data to SSTable")

	// 获取被删除的 key
	value, err = sstable.Get("key2")
	assert.NoError(t, err, "Failed to get value for deleted key")
	assert.Nil(t, value, "Value for deleted key should be nil")
}

func TestLoadSSTable(t *testing.T) {
	// Setup a new SSTable with temporary directory
	tempDir := "D://platodb//"

	sstable, err := NewSSTable(tempDir, context.Background())
	assert.NoError(t, err, "Failed to create SSTable")

	segfiles, _ := filepath.Glob(path.Join("D://platodb", "*.seg"))

	// Check that the segments are loaded
	assert.Len(t, sstable.Segments, len(segfiles), "Should have 1 segment loaded")
}

func TestGenerateSegmentId(t *testing.T) {
	// Setup SSTable
	tempDir := "D://platodb//"
	sstable, err := NewSSTable(tempDir, context.Background())
	assert.NoError(t, err)

	// Generate new segment ID
	newID := sstable.generateSegmentId()
	assert.Equal(t, sstable.Segments[len(sstable.Segments)-1].id+1, newID, "The first segment ID should be 1")

	// Add a segment and generate again
	segment := &segment{id: 1}
	sstable.Segments = append(sstable.Segments, segment)
	newID = sstable.generateSegmentId()
	assert.Equal(t, int64(2), newID, "The second segment ID should be 2")
}
