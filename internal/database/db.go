package database

import (
	"github.com/Jasonbourne723/platodb/internal/database/memorytable"
)

type DB struct {
	MemoryTable memorytable.Memorytable
}

type Options func(db *DB)

func NewDB(options ...Options) (*DB, error) {
	db := DB{
		MemoryTable: memorytable.NewSkipTable(),
	}

	for _, option := range options {
		option(&db)
	}
	return &db, nil
}

func (db *DB) Get(key string) ([]byte, error) {

	return db.MemoryTable.Get(key), nil
}

func (db *DB) Set(key string, value []byte) error {

	db.MemoryTable.Set(key, value)
	return nil
}

func (db *DB) Del(key string) error {
	db.MemoryTable.Del(key)
	return nil
}
