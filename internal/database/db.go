package database

import (
	"errors"

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

	if len(key) == 0 {
		return nil, errors.New("key 格式错误")
	}
	return db.MemoryTable.Get(key), nil
}

func (db *DB) Set(key string, value []byte) error {

	if len(key) == 0 {
		return errors.New("key 格式错误")
	}

	db.MemoryTable.Set(key, value)
	return nil
}
