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
	memoryTables    []memorytable.MemoryTable
	sstable         *sstable.SSTable
	memoryTableLock *sync.RWMutex
	flushLock       *sync.Mutex
	isFlushing      bool
	isShutdonw      int32
	walMap          map[memorytable.MemoryTable]wal.WriterCloser
	segmentSize     int64
	dataDir         string
	walDir          string
}

// Options defines a function type that accepts a pointer to DB and modifies its configuration.
// It is used to customize the behavior of a DB instance during initialization.
type Options func(db *DB)

// NewDB initializes and returns a new instance of DB with optional configuration provided by variadic Options.
// It sets up the initial memory tables, SSTable, and recovers data from the Write-Ahead Log (WAL) if present.
// Returns a pointer to DB and an error if any occurs during setup or recovery.
func NewDB(options ...Options) (*DB, error) {

	db := DB{
		memoryTables:    make([]memorytable.MemoryTable, 0, 2),
		walMap:          make(map[memorytable.MemoryTable]wal.WriterCloser),
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

// Dir sets the data directory and write-ahead log (WAL) directory options for the database.
// This function is intended to be used when configuring a database instance.
// It takes two string arguments representing the paths for the data directory and WAL directory, respectively.
// The function returns an Options func that applies these settings to a DB instance.
func Dir(dataDir string, walDir string) Options {
	return func(db *DB) {
		db.dataDir = dataDir
		db.walDir = walDir
	}
}

// SegmentSize sets the size limit for each data segment in megabytes.
// The provided value is converted to bytes before being applied to the DB configuration.
// This function is intended to be used as an option when initializing a database instance.
func SegmentSize(segmentSize int32) Options {
	return func(db *DB) {
		db.segmentSize = int64(segmentSize * common.MB)
	}
}

// Get retrieves the value associated with the specified key from the database.
// It first checks the memory tables in reverse order and then falls back to the SSTable.
// If the database is shutting down, it returns an error.
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

// Set stores the given value for the specified key in the database.
// It writes the data to the Write-Ahead Log (WAL) if enabled and updates the in-memory table.
// If the in-memory table size exceeds the defined segment size, a flush operation is initiated.
// Returns an error if the database is shutting down.
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

// Del deletes the entry associated with the provided key from the database.
// It writes a deletion record to the Write-Ahead Log (WAL), if enabled, and marks the entry as deleted in the in-memory table.
// An error is returned if the database is in the process of shutting down.
func (db *DB) Del(key string) error {

	if atomic.LoadInt32(&db.isShutdonw) == 1 {
		return errors.New("database is shutting down")
	}

	db.memoryTableLock.RLocker().Lock()
	defer db.memoryTableLock.RLocker().Unlock()
	memoryTable := db.memoryTables[len(db.memoryTables)-1]

	if walWriter, ok := db.walMap[memoryTable]; ok {
		err := walWriter.Write(&common.Chunk{
			Key:     key,
			Value:   nil,
			Deleted: true,
		})
		if err != nil {
			return err
		}
	}

	memoryTable.Set(key, nil, true)
	return nil
}

// Shutdown initiates the shutdown process for the database.
// It prevents new operations by setting the shutdown flag and flushes remaining memory tables to disk.
// Afterward, it closes the SSTable to finalize the shutdown sequence.
// This method is idempotent and will return immediately if called again after the shutdown has been initiated.
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

// createMemoryTable initializes a new memory table, appends it to the database's memoryTables slice,
// and associates a new Write-Ahead Log (WAL) writer with it.
// Returns an error if the WAL writer creation fails.
func (db *DB) createMemoryTable() error {
	memoryTable := memorytable.NewMemoryTable()
	db.memoryTables = append(db.memoryTables, memoryTable)
	walWriterCloser, err := wal.NewWriterCloser(db.walDir)
	if err != nil {
		return err
	}
	db.walMap[memoryTable] = walWriterCloser
	return nil
}

// removeMemoryTable removes the first memory table from the database, closes its associated WAL writer,
// and updates the memoryTables slice accordingly. This is typically done after successfully flushing
// the memory table's contents to the SSTable.
func (db *DB) removeMemoryTable() {
	db.walMap[db.memoryTables[0]].Close()
	delete(db.walMap, db.memoryTables[0])
	db.memoryTables = db.memoryTables[1:]
}

// initiateFlush starts the process of flushing the in-memory data to disk if not already flushing.
// It creates a new memory table, acquires necessary locks, and triggers the flush routine.
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

// flush persists the first memory table to the SSTable, removes it from memory,
// and updates internal state accordingly. This function should be called when a memory table is ready to be flushed to disk.
// It acquires a write lock on the memory table to ensure thread safety during the flush operation.
func (db *DB) flush() {
	if err := db.sstable.Write(db.memoryTables[0]); err != nil {
		log.Fatal(fmt.Errorf("内存表持久化异常：%w", err))
	}
	db.memoryTableLock.Lock()
	defer db.memoryTableLock.Unlock()
	db.removeMemoryTable()
}

// recoverFromWal recovers the database state from Write-Ahead Log (WAL) files in the specified directory.
// It ensures the directory exists, iterates through each WAL file, reads chunks,
// applies them to a memory table, and writes the memory table to the SSTable if non-empty.
// Returns an error if any step fails, such as I/O issues or failures during recovery.
func (db *DB) recoverFromWal(walDir string) error {

	if err := common.EnsureDirExists(walDir); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(walDir, "*.log"))
	if err != nil {
		return err
	}
	for _, walFilePath := range files {

		walReaderCloser, err := wal.NewReaderCloser(walFilePath)
		if err != nil {
			return err
		}
		memoryTable := memorytable.NewMemoryTable()
		for {
			chunk, err := walReaderCloser.Read()
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
		if err := walReaderCloser.Close(); err != nil {
			return err
		}
	}

	return nil
}
