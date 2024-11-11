package database

import (
	"fmt"
	"log"
	"sync"

	"github.com/Jasonbourne723/platodb/internal/database/memorytable"
	"github.com/Jasonbourne723/platodb/internal/database/sstable"
)

type DB struct {
	memoryTables    []memorytable.Memorytable
	sstable         *sstable.SSTable
	memoryTableLock *sync.RWMutex
}

type Options func(db *DB)

// 创建DB
func NewDB(options ...Options) (*DB, error) {

	sst, err := sstable.NewSSTable()
	if err != nil {
		return nil, fmt.Errorf("sstable加载失败:%w", err)
	}

	db := DB{
		memoryTables: make([]memorytable.Memorytable, 0, 2),
		sstable:      sst,
	}
	db.memoryTables = append(db.memoryTables, memorytable.NewSkipTable())

	for _, option := range options {
		option(&db)
	}
	return &db, nil
}

// 查询key
func (db *DB) Get(key string) ([]byte, error) {

	db.memoryTableLock.RLocker().Lock()
	defer db.memoryTableLock.RLocker().Unlock()

	for i := len(db.memoryTables) - 1; i >= 0; i-- {
		val := db.memoryTables[i].Get(key)
		if val != nil {
			return val, nil
		}
	}

	return db.sstable.Get(key)
}

// 写入key-value
func (db *DB) Set(key string, value []byte) error {

	db.memoryTableLock.RLocker().Lock()
	defer db.memoryTableLock.RLocker().Unlock()

	db.memoryTables[0].Set(key, value)
	if db.memoryTables[0].Size() > sstable.SEGMENT_SIZE {
		go db.Flush()
	}

	return nil
}

// 删除key
func (db *DB) Del(key string) error {

	db.memoryTableLock.RLocker().Lock()
	defer db.memoryTableLock.RLocker().Unlock()

	db.memoryTables[len(db.memoryTables)-1].Del(key)
	return nil
}

// 内存表写入sstable
func (db *DB) Flush() {

	db.memoryTableLock.Lock()
	db.memoryTables = append(db.memoryTables, memorytable.NewSkipTable())
	db.memoryTableLock.Unlock()

	if err := db.sstable.Write(db.memoryTables[0]); err != nil {
		log.Fatal(fmt.Errorf("内存表持久化异常：%w", err))
	}

	db.memoryTableLock.Lock()
	db.memoryTables = db.memoryTables[1:]
	db.memoryTableLock.Unlock()

	//删除wal
}
