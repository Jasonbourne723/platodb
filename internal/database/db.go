package database

import (
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Jasonbourne723/platodb/internal/database/common"
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
	isShutdonw      int32
	walMap          map[memorytable.Memorytable]wal.WalWriterCloser
	segmentSize     int64
	dataDir         string
	walDir          string
}

type Options func(db *DB)

// 创建DB
func NewDB(options ...Options) (*DB, error) {

	db := DB{
		memoryTables:    make([]memorytable.Memorytable, 0, 2),
		walMap:          make(map[memorytable.Memorytable]wal.WalWriterCloser),
		sstable:         nil,
		memoryTableLock: &sync.RWMutex{},
		flushLock:       &sync.Mutex{},
		isFlushing:      false,
		segmentSize:     8 * common.MB,
		dataDir:         "/var/platodb",
		walDir:          "/var/platodb/wal",
	}

	for _, option := range options {
		option(&db)
	}

	sst, err := sstable.NewSSTable(db.dataDir)
	if err != nil {
		return nil, fmt.Errorf("sstable加载失败:%w", err)
	}
	db.sstable = sst

	if err := db.recoverFromWal(db.walDir); err != nil {
		return nil, err
	}
	if err := db.createMemoryTable(); err != nil {
		return nil, err
	}
	return &db, nil
}

func Dir(dataDir string, walDir string) Options {
	return func(db *DB) {
		db.dataDir = dataDir
		db.walDir = walDir
	}
}

func SegmentSize(segmentSize int32) Options {
	return func(db *DB) {
		db.segmentSize = int64(segmentSize * common.MB)
	}
}

// 查询key
func (db *DB) Get(key string) ([]byte, error) {

	if atomic.LoadInt32(&db.isShutdonw) == 1 {
		return nil, errors.New("database is shutting down")
	}

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

	if atomic.LoadInt32(&db.isShutdonw) == 1 {
		return errors.New("database is shutting down")
	}
	db.memoryTableLock.RLocker().Lock()
	memeryTable := db.memoryTables[len(db.memoryTables)-1]

	if wal, ok := db.walMap[memeryTable]; ok {
		wal.Write(&common.Chunk{
			Key:     key,
			Value:   value,
			Deleted: false,
		})
	}
	db.memoryTableLock.RLocker().Unlock()
	memeryTable.Set(key, value, false)
	if memeryTable.Size() > db.segmentSize {
		db.initiateFlush()
	}

	return nil
}

// 删除key
func (db *DB) Del(key string) error {

	if atomic.LoadInt32(&db.isShutdonw) == 1 {
		return errors.New("database is shutting down")
	}

	db.memoryTableLock.RLocker().Lock()
	defer db.memoryTableLock.RLocker().Unlock()
	memeryTable := db.memoryTables[len(db.memoryTables)-1]

	if wal, ok := db.walMap[memeryTable]; ok {
		wal.Write(&common.Chunk{
			Key:     key,
			Value:   nil,
			Deleted: true,
		})
	}

	memeryTable.Set(key, nil, true)
	return nil
}

// 优雅关机
func (db *DB) Shutdown() {

	if !atomic.CompareAndSwapInt32(&db.isShutdonw, 0, 1) {
		return
	}

	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	for len(db.memoryTables) > 0 {
		db.flush()
	}

	db.sstable.Close()
}

func (db *DB) createMemoryTable() error {
	memoryTable := memorytable.NewMemoryTable()
	db.memoryTables = append(db.memoryTables, memoryTable)
	wal, err := wal.NewWalWriterCloser(db.walDir)
	if err != nil {
		return err
	}
	db.walMap[memoryTable] = wal
	return nil
}

func (db *DB) removeMemoryTable() {
	db.walMap[db.memoryTables[0]].Close()
	delete(db.walMap, db.memoryTables[0])
	db.memoryTables = db.memoryTables[1:]
}

// 初始化flush
func (db *DB) initiateFlush() {
	db.flushLock.Lock()
	defer db.flushLock.Unlock()

	if db.isFlushing {
		return
	}
	db.isFlushing = true

	db.memoryTableLock.Lock()
	defer db.memoryTableLock.Unlock()
	if err := db.createMemoryTable(); err != nil {
		log.Fatal(fmt.Errorf("创建内存表失败，%w", err))
	}

	go func() {
		db.flush()
		db.flushLock.Lock()
		db.isFlushing = false
		db.flushLock.Unlock()
	}()
}

// 内存表写入sstable
func (db *DB) flush() {
	if err := db.sstable.Write(db.memoryTables[0]); err != nil {
		log.Fatal(fmt.Errorf("内存表持久化异常：%w", err))
	}
	db.memoryTableLock.Lock()
	defer db.memoryTableLock.Unlock()
	db.removeMemoryTable()
}

// 崩溃恢复
func (db *DB) recoverFromWal(walDir string) error {

	if err := common.EnsureDirExists(walDir); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(walDir, "*.log"))
	if err != nil {
		return err
	}
	for _, walFilePath := range files {

		wal, err := wal.NewWalReaderCloser(walFilePath)
		if err != nil {
			return err
		}
		memoryTable := memorytable.NewMemoryTable()
		for {
			chunk, err := wal.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if chunk == nil {
				break
			}
			memoryTable.Set(chunk.Key, chunk.Value, chunk.Deleted)
		}
		if memoryTable.Size() > 0 {
			if err := db.sstable.Write(memoryTable); err != nil {
				return err
			}
		}
		if err := wal.Close(); err != nil {
			return err
		}
	}

	return nil
}
