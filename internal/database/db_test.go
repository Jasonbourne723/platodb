package database

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_SetAndGet(t *testing.T) {
	db, _ := NewDB()

	// 设置 key-value 对
	key := "test-key"
	value := []byte("test-value")
	err := db.Set(key, value)
	assert.NoError(t, err)

	// 获取 key-value 对
	retrievedValue, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// 删除 key
	err = db.Del(key)
	assert.NoError(t, err)

	// 确认 key 已删除
	retrievedValue, err = db.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, retrievedValue)
}
