package sstable

import "os"

const (
	Root = "D://platodb//"
)

type SSTable struct {
	Segments []*Segment
	Root     string
}

type Scanner interface {
	Scan() bool
	ScanValue() (key string, value []byte, deleted bool)
}

func NewSSTable() (*SSTable, error) {

	sst := &SSTable{
		Root:     Root,
		Segments: make([]*Segment, 0, 10),
	}
	err := sst.Load()
	if err != nil {
		return nil, err
	}
	return sst, nil
}

//加载sstable信息
func (s *SSTable) Load() error {
	files, err := os.ReadDir(s.Root)
	if err != nil {
		return err
	}

	for _, file := range files {

		name := file.Name()

		seg, err := LoadSegment(s.Root, name)
		if err != nil {
			continue
		}
		s.Segments = append(s.Segments, seg)
	}

	return nil
}

//生成下一个segmentId
func (s *SSTable) generateSegmentId() int64 {
	if len(s.Segments) == 0 {
		return 0
	}
	return s.Segments[len(s.Segments)-1].id + 1
}

//将内存表写入sstable
func (s *SSTable) Write(scanner Scanner) error {

	seg, err := NewSegment(s.Root, s.generateSegmentId())
	if err != nil {
		return err
	}

	if scanner.Scan() {
		key, value, deleted := scanner.ScanValue()
		chunk := &Chunk{
			key:     key,
			value:   value,
			deleted: deleted,
		}
		seg.Write(chunk)
	}

	s.Segments = append(s.Segments, seg)
	return seg.Sync()
}

//倒序扫描segment文件，直到查询key
func (s *SSTable) Get(key string) ([]byte, error) {

	for i := len(s.Segments) - 1; i >= 0; i-- {
		chunk, err := s.Segments[i].Get(key)
		if err != nil {
			return nil, err
		}
		if chunk != nil {
			if chunk.deleted {
				return nil, nil
			}
			return chunk.value, nil
		}
	}
	return nil, nil
}
