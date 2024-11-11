package memorytable

import "math/rand"

type SkipTable struct {
	maxLevel int32
	head     *node
	level    int32
	allSize  int64
	scanPos  *node
}

type node struct {
	level   int32
	key     string
	value   []byte
	next    []*node
	deleted bool
}

func NewSkipTable() *SkipTable {
	var maxLevel int32 = 10
	return &SkipTable{
		maxLevel: maxLevel,
		level:    1,
		head:     NewNode(maxLevel),
	}
}

//新建节点
func NewNode(level int32) *node {
	return &node{
		level: level,
		next:  make([]*node, level),
		key:   "",
	}
}

//插入数据
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
	newNode.key = key
	newNode.value = value
	newNode.deleted = false

	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && key >= node.next[i].key {
			node = node.next[i]
		}
		if level > i {
			if node.key == key {
				node.value = value
				node.deleted = false
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

//查询数据
func (s *SkipTable) Get(key string) []byte {

	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && node.next[i].key <= key {
			if node.next[i].key == key {
				if node.next[i].deleted {
					return nil
				}
				return node.next[i].value
			} else {
				node = node.next[i]
			}
		}
	}

	return nil
}

//删除数据
func (s *SkipTable) Del(key string) {
	node := s.head
	for i := s.level - 1; i >= 0; i-- {
		for node.next[i] != nil && node.next[i].key <= key {
			if node.next[i].key == key {
				node.next[i].deleted = true
				return
			} else {
				node = node.next[i]
			}
		}
	}
}

//随机level
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

//内存数据字节数
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

func (s *SkipTable) ScanValue() (key string, value []byte, deleted bool) {
	return s.scanPos.key, s.scanPos.value, s.scanPos.deleted
}
