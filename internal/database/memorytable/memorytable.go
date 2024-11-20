package memorytable

import (
	"math/rand"
	"sync"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

type MemoryTable interface {
	Set(key string, value []byte, deleted bool)
	Get(key string) []byte
	Size() int64
	common.Scanner
}

type DefaultMemoryTable struct {
	maxLevel int32
	head     *Node
	level    int32
	allSize  int64
	scanPos  *Node
	lock     *sync.RWMutex
}

type Node struct {
	level int32
	next  []*Node
	chunk *common.Chunk
}

func NewMemoryTable() *DefaultMemoryTable {
	var maxLevel int32 = 10
	t := &DefaultMemoryTable{
		maxLevel: maxLevel,
		level:    1,
		head:     NewNode(maxLevel),
		lock:     &sync.RWMutex{},
	}
	t.scanPos = t.head
	return t
}

// NewNode creates and returns a new node with the specified level.
// The node's next slice is initialized to have the capacity equal to the level,
// and its chunk is initialized with default values.
// Parameters:
// level (int32): The level of the node to be created.
// Returns:
// *node: A pointer to the newly created node.
func NewNode(level int32) *Node {
	return &Node{
		level: level,
		next:  make([]*Node, level),
		chunk: &common.Chunk{
			Key:     "",
			Value:   nil,
			Deleted: false,
		},
	}
}

// Set adds or updates a key-value pair in the DefaultMemoryTable. It also handles deletion by setting the 'deleted' flag.
// It dynamically adjusts the skip list levels based on randomness up to the maxLevel defined for the table.
// The method is thread-safe and ensures concurrent access is properly managed.
// Parameters:
// key (string): The key for the entry.
// value ([]byte): The value associated with the key.
// deleted (bool): A flag indicating whether the entry is marked as deleted.
// It returns nothing.
func (s *DefaultMemoryTable) Set(key string, value []byte, deleted bool) {

	s.lock.Lock()
	defer s.lock.Unlock()

	var level int32
	if s.head.next[0] == nil {
		level = 1
	} else {
		level = s.randomLevel()
	}

	if level > s.level {
		if s.level < s.maxLevel {
			s.level++
			level = s.level
		}
	}

	var newNode = NewNode(level)
	newNode.chunk.Key = key
	newNode.chunk.Value = value
	newNode.chunk.Deleted = deleted

	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && key >= node.next[i].chunk.Key {
			node = node.next[i]
		}
		if level > i {
			if node.chunk.Key == key {
				node.chunk.Value = value
				node.chunk.Deleted = deleted
			} else {
				if node.next[i] == nil {
					node.next[i] = newNode
				} else {
					next := node.next[i]
					node.next[i] = newNode
					newNode.next[i] = next
				}
			}
		}
	}
	s.allSize = s.allSize + int64(len(key)) + int64(len(value))
}

// Get retrieves the value associated with the provided key from the DefaultMemoryTable. If the key exists and is not marked as deleted, the corresponding value is returned. Otherwise, nil is returned.
// This method is designed to be thread-safe, allowing concurrent reads while write operations are in progress.
// Parameters:
// key (string): The key to look up in the table.
// Returns:
// []byte: The value associated with the key if found and not deleted, otherwise nil.
func (s *DefaultMemoryTable) Get(key string) []byte {

	s.lock.RLocker().Lock()
	defer s.lock.RLocker().Unlock()

	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && node.next[i].chunk.Key <= key {
			if node.next[i].chunk.Key == key {
				if node.next[i].chunk.Deleted {
					return nil
				}
				return node.next[i].chunk.Value
			} else {
				node = node.next[i]
			}
		}
	}

	return nil
}

// randomLevel generates a random level for a new node in the skip list.
// The level is determined by flipping a coin until it lands tails or the maximum level is reached.
// Returns an int32 representing the generated level.
func (s *DefaultMemoryTable) randomLevel() int32 {

	level := 1

	for i := 0; i < int(s.maxLevel); i++ {
		if rand.Int()%2 == 1 {
			level++
		} else {
			break
		}
	}
	return int32(level)
}

// Size returns the total size in bytes of all key-value pairs stored in the DefaultMemoryTable.
// This includes the combined lengths of keys and values, not accounting for internal structure overhead.
// The method is safe for concurrent use.
func (s *DefaultMemoryTable) Size() int64 {

	s.lock.RLocker().Lock()
	defer s.lock.RLocker().Unlock()

	return s.allSize
}

// Scan advances the scanner to the next entry in the DefaultMemoryTable and reports whether there was a value to scan.
// It returns false when the scan ends, either by reaching the end of the table or due to an error.
// This method should be used in conjunction with ScanValue to retrieve the actual data.
func (s *DefaultMemoryTable) Scan() bool {
	if s.scanPos.next[0] == nil {
		return false
	}
	s.scanPos = s.scanPos.next[0]
	return true
}

// ScanValue returns the current chunk data at the scanner's position in the DefaultMemoryTable.
// This method should be used after Scan to obtain the value of the entry pointed by the scanner.
// Returns a pointer to common.Chunk which holds the key-value pair information.
func (s *DefaultMemoryTable) ScanValue() *common.Chunk {
	return s.scanPos.chunk
}
