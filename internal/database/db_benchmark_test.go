package database

import (
	_ "crypto/rand"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	numEntries = 100000 // 预填充的数据量
	valueSize  = 256    // 每个 value 的大小（字节数）
)

// 初始化数据库并预填充数据
func setupDB(b *testing.B) *DB {
	db, err := NewDB()
	if err != nil {
		b.Fatalf("Failed to initialize database: %v", err)
	}

	rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key%d", i)
		value := make([]byte, valueSize)
		rand.Read(value) // 随机生成 value
		if err := db.Set(key, value); err != nil {
			b.Fatalf("Failed to set key: %v", err)
		}
	}

	b.Logf("Database preloaded with %d entries", numEntries)
	return db
}

// Benchmark 测试 Set 操作
func BenchmarkSet(b *testing.B) {
	db := setupDB(b)

	b.ResetTimer() // 重置计时器，只统计核心代码运行时间
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("new_key%d", i)
		value := make([]byte, valueSize)
		rand.Read(value)
		if err := db.Set(key, value); err != nil {
			b.Fatalf("Set operation failed: %v", err)
		}
	}
}

// Benchmark 测试 Get 操作
func BenchmarkGet(b *testing.B) {
	db := setupDB(b)

	// 随机生成需要查询的 keys
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = fmt.Sprintf("key%d", rand.Intn(numEntries))
	}

	b.ResetTimer() // 重置计时器
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i]); err != nil {
			b.Fatalf("Get operation failed: %v", err)
		}
	}
}
