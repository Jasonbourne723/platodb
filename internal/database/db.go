package database

import (
	"fmt"

	"github.com/Jasonbourne723/platodb/internal/database/memorytable"
	"github.com/Jasonbourne723/platodb/internal/database/sstable"
)

type DB struct {
	memoryTable memorytable.Memorytable
	sstable     *sstable.SSTable
}

type Options func(db *DB)

func NewDB(options ...Options) (*DB, error) {

	sst, err := sstable.NewSSTable()
	if err != nil {
		return nil, fmt.Errorf("sstable加载失败:%w", err)
	}

	db := DB{
		memoryTable: memorytable.NewSkipTable(),
		sstable:     sst,
	}

	for _, option := range options {
		option(&db)
	}
	return &db, nil
}

func (db *DB) Get(key string) ([]byte, error) {

	val := db.memoryTable.Get(key)
	if val != nil {
		return val, nil
	}
	return db.sstable.Get(key)
}

func (db *DB) Set(key string, value []byte) error {

	db.memoryTable.Set(key, value)
	return nil
}

func (db *DB) Del(key string) error {
	db.memoryTable.Del(key)
	return nil
}
