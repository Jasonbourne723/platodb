package memorytable

import (
	"math/rand"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

type SkipTable struct {
	maxLevel int32
	head     *node
	level    int32
	allSize  int64
	scanPos  *node
}

type node struct {
	level int32
	next  []*node
	chunk *common.Chunk
}

func NewSkipTable() *SkipTable {
	var maxLevel int32 = 10
	return &SkipTable{
		maxLevel: maxLevel,
		level:    1,
		head:     NewNode(maxLevel),
	}
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
func (s *SkipTable) Set(key string, value []byte) {

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
	newNode.chunk.Deleted = false

	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && key >= node.next[i].chunk.Key {
			node = node.next[i]
		}
		if level > i {
			if node.chunk.Key == key {
				node.chunk.Value = value
				node.chunk.Deleted = false
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
func (s *SkipTable) Get(key string) []byte {

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

// 删除数据
func (s *SkipTable) Del(key string) {
	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && node.next[i].chunk.Key <= key {
			if node.next[i].chunk.Key == key {
				node.next[i].chunk.Deleted = true
				return
			} else {
				node = node.next[i]
			}
		}
	}
}

// 随机level
func (s *SkipTable) randomLevel() int32 {

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
func (s *SkipTable) Size() int64 {
	return s.allSize
}

func (s *SkipTable) Scan() bool {
	if s.scanPos.next[0] == nil {
		return false
	}
	s.scanPos = s.scanPos.next[0]
	return true
}

func (s *SkipTable) ScanValue() *common.Chunk {
	return s.scanPos.chunk
}
