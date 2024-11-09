package memorytable

import "math/rand"

type SkipTable struct {
	MaxLevel int32
	Head     *Node
	Level    int32
	Size     int64
}

type Node struct {
	Level   int32
	Key     string
	Value   []byte
	Next    []*Node
	Deleted bool
}

func NewSkipTable() *SkipTable {
	var maxLevel int32 = 10
	return &SkipTable{
		MaxLevel: maxLevel,
		Level:    1,
		Head:     NewNode(maxLevel),
	}
}

//新建节点
func NewNode(level int32) *Node {
	return &Node{
		Level: level,
		Next:  make([]*Node, level),
		Key:   "",
	}
}

//插入数据
func (s *SkipTable) Set(key string, value []byte) {

	var level int32
	if s.Head.Next[0] == nil {
		level = 1
	} else {
		level = s.randomLevel()
	}

	if level > s.Level {
		if s.Level < s.MaxLevel {
			s.Level++
			level = s.Level
		}
	}

	var newNode = NewNode(level)
	newNode.Key = key
	newNode.Value = value
	newNode.Deleted = false

	node := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for node.Next[i] != nil && key >= node.Next[i].Key {
			node = node.Next[i]
		}
		if level > i {
			if node.Key == key {
				node.Value = value
				node.Deleted = false
			} else {
				if node.Next[i] == nil {
					node.Next[i] = newNode
				} else {
					next := node.Next[i]
					node.Next[i] = newNode
					newNode.Next[i] = next
				}
			}
		}
	}
	s.Size = s.Size + int64(len(key)) + int64(len(value))
}

//查询数据
func (s *SkipTable) Get(key string) []byte {

	node := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for node.Next[i] != nil && node.Next[i].Key <= key {
			if node.Next[i].Key == key {
				if node.Next[i].Deleted {
					return nil
				}
				return node.Next[i].Value
			} else {
				node = node.Next[i]
			}
		}
	}

	return nil
}

func (s *SkipTable) Del(key string) {
	node := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for node.Next[i] != nil && node.Next[i].Key <= key {
			if node.Next[i].Key == key {
				node.Next[i].Deleted = true
				return
			} else {
				node = node.Next[i]
			}
		}
	}
}

//随机level
func (s *SkipTable) randomLevel() int32 {

	level := 1

	for i := 0; i < int(s.MaxLevel); i++ {
		if rand.Int()%2 == 1 {
			level++
		} else {
			break
		}
	}
	return int32(level)
}

//内存数据字节数
func (s *SkipTable) Length() int64 {
	return s.Size
}
