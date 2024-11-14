package memorytable

import (
	"math/rand"
	"sync"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

type Memorytable interface {
	Set(key string, value []byte, deleted bool)
	Get(key string) []byte
	Size() int64
	common.Scanner
}

type DefaultMemoryTable struct {
	maxLevel int32
	head     *node
	level    int32
	allSize  int64
	scanPos  *node
	lock     *sync.RWMutex
}

type node struct {
	level int32
	next  []*node
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

// 新建节点
func NewNode(level int32) *node {
	return &node{
		level: level,
		next:  make([]*node, level),
		chunk: &common.Chunk{
			Key:     "",
			Value:   nil,
			Deleted: false,
		},
	}
}

// 插入数据
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

// 查询数据
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

// 随机level
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

// 内存数据字节数
func (s *DefaultMemoryTable) Size() int64 {

	s.lock.RLocker().Lock()
	defer s.lock.RLocker().Unlock()

	return s.allSize
}

func (s *DefaultMemoryTable) Scan() bool {
	if s.scanPos.next[0] == nil {
		return false
	}
	s.scanPos = s.scanPos.next[0]
	return true
}

func (s *DefaultMemoryTable) ScanValue() *common.Chunk {
	return s.scanPos.chunk
}
