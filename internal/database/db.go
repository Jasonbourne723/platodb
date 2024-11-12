package database

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/Jasonbourne723/platodb/internal/database/memorytable"
	"github.com/Jasonbourne723/platodb/internal/database/sstable"
	"github.com/Jasonbourne723/platodb/internal/database/wal"
)

type DB struct {
	memoryTables    []memorytable.Memorytable
	sstable         *sstable.SSTable
	memoryTableLock *sync.RWMutex
	flushLock       *sync.Mutex
	isFlushing      bool
	walMap          map[*memorytable.Memorytable]wal.Wal
}

type Options func(db *DB)

// 创建DB
func NewDB(options ...Options) (*DB, error) {

	sst, err := sstable.NewSSTable()
	if err != nil {
		return nil, fmt.Errorf("sstable加载失败:%w", err)
	}

	db := DB{
		memoryTables:    make([]memorytable.Memorytable, 0, 2),
		walMap:          make(map[*memorytable.Memorytable]wal.Wal),
		sstable:         sst,
		memoryTableLock: &sync.RWMutex{},
		flushLock:       &sync.Mutex{},
		isFlushing:      false,
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

	db.memoryTables[len(db.memoryTables)-1].Set(key, value)
	if db.memoryTables[len(db.memoryTables)-1].Size() > sstable.SEGMENT_SIZE {
		db.initiateFlush()
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

func (db *DB) initiateFlush() {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	if db.isFlushing {
		return
	}
	db.isFlushing = true

	go func() {
		db.Flush()
		db.flushLock.Lock()
		db.isFlushing = false
		db.flushLock.Unlock()
	}()
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

func (db *DB) recoverFromWal(walDir string) error {
	files, err := filepath.Glob(filepath.Join(walDir, "*.wal"))
	if err != nil {
		return err
	}
	for _, walFile := range files {
		// 为每个 WAL 文件创建一个新的内存表，并将数据重放到内存表中
		memTable := memorytable.NewSkipTable()
		w, err := wal.OpenWAL(walFile) // 假设 OpenWAL 以读取模式打开 WAL 文件
		if err != nil {
			return fmt.Errorf("打开 WAL 文件失败 %s: %w", walFile, err)
		}
		db.memoryTables = append(db.memoryTables, memTable)
		db.walTableMap[memTable] = w

		// 重放 WAL 日志到内存表
		for {
			key, value, err := w.ReadEntry()
			if err == wal.ErrEOF {
				break
			}
			if err != nil {
				return fmt.Errorf("读取 WAL 日志失败 %s: %w", walFile, err)
			}
			memTable.Set(key, value)
		}
	}
}
