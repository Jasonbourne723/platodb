package database

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"

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
	walMap          map[memorytable.Memorytable]wal.WalWriterCloser
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
		walMap:          make(map[memorytable.Memorytable]wal.WalWriterCloser),
		sstable:         sst,
		memoryTableLock: &sync.RWMutex{},
		flushLock:       &sync.Mutex{},
		isFlushing:      false,
	}

	if err := db.recoverFromWal("D://platodb//wal//"); err != nil {
		return nil, err
	}
	if err := db.createMemoryTable(); err != nil {
		return nil, err
	}

	for _, option := range options {
		option(&db)
	}
	return &db, nil
}

func (db *DB) createMemoryTable() error {
	memoryTable := memorytable.NewSkipTable()
	db.memoryTables = append(db.memoryTables, memoryTable)
	wal, err := wal.NewWalWriterCloser()
	if err != nil {
		return err
	}
	db.walMap[memoryTable] = wal
	return nil
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

	memeryTable := db.memoryTables[len(db.memoryTables)-1]

	if wal, ok := db.walMap[memeryTable]; ok {
		wal.Write(&common.Chunk{
			Key:     key,
			Value:   value,
			Deleted: false,
		})
	}

	memeryTable.Set(key, value, false)
	if memeryTable.Size() > sstable.SEGMENT_SIZE {
		db.initiateFlush()
	}

	return nil
}

// 删除key
func (db *DB) Del(key string) error {

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
	if err := db.createMemoryTable(); err != nil {
		log.Fatal(fmt.Errorf("创建内存表失败，%w", err))
	}
	db.memoryTableLock.Unlock()

	log.Println("开始flush")

	if err := db.sstable.Write(db.memoryTables[0]); err != nil {
		log.Fatal(fmt.Errorf("内存表持久化异常：%w", err))
	}

	log.Println("flush结束")

	db.memoryTableLock.Lock()
	db.walMap[db.memoryTables[0]].Close()
	delete(db.walMap, db.memoryTables[0])
	db.memoryTables = db.memoryTables[1:]
	db.memoryTableLock.Unlock()
}

// 崩溃恢复
func (db *DB) recoverFromWal(walDir string) error {

	files, err := filepath.Glob(filepath.Join(walDir, "*.log"))
	if err != nil {
		return err
	}
	for _, walFilePath := range files {

		wal, err := wal.NewWalReaderCloser(walFilePath)
		if err != nil {
			return err
		}
		memoryTable := memorytable.NewSkipTable()
		log.Println("wal崩溃恢复开始")
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
			//log.Printf("wal数据恢复,key:%v,value,%v,deleted:%v", chunk.Key, chunk.Value, chunk.Deleted)
			memoryTable.Set(chunk.Key, chunk.Value, chunk.Deleted)
		}
		log.Println("wal崩溃恢复结束")
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
