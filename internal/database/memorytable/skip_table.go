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
	Forward []*Node
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
		Level:   level,
		Forward: make([]*Node, level),
		Key:     "",
	}
}

//插入数据
func (s *SkipTable) Set(key string, value []byte) {

	var level int32
	if s.Head.Forward[0] == nil {
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

	node := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for node.Forward[i] != nil && node.Forward[i].Key < key {
			node = node.Forward[i]
		}
		if level > i {
			if node.Forward[i] == nil {
				node.Forward[i] = newNode
			} else {
				next := node.Forward[i]
				node.Forward[i] = newNode
				newNode.Forward[i] = next
			}
		}
	}
	s.Size = s.Size + int64(len(key)) + int64(len(value))
}

//查询数据
func (s *SkipTable) Get(key string) []byte {

	node := s.Head
	for i := s.Level - 1; i >= 0; i-- {
		for node.Forward[i] != nil && node.Forward[i].Key <= key {
			if node.Forward[i].Key == key {
				return node.Forward[i].Value
			} else {
				node = node.Forward[i]
			}
		}
	}

	return nil
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
