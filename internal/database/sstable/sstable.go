package sstable

import (
	"os"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

type SSTable struct {
	Segments []*segment
	Root     string
}

// 创建sstable
func NewSSTable(root string) (*SSTable, error) {

	sst := &SSTable{
		Root:     root,
		Segments: make([]*segment, 0, 10),
	}
	err := sst.load()
	if err != nil {
		return nil, err
	}
	return sst, nil
}

// 加载sstable信息
func (s *SSTable) load() error {

	if err := common.EnsureDirExists(s.Root); err != nil {
		return err
	}

	files, err := os.ReadDir(s.Root)
	if err != nil {
		return err
	}

	for _, file := range files {

		name := file.Name()

		seg, err := loadSegment(s.Root, name)
		if err != nil {
			continue
		}
		s.Segments = append(s.Segments, seg)
	}

	return nil
}

// 生成下一个segmentId
func (s *SSTable) generateSegmentId() int64 {
	if len(s.Segments) == 0 {
		return 1
	}
	return s.Segments[len(s.Segments)-1].id + 1
}

// 将内存表写入sstable
func (s *SSTable) Write(scanner common.Scanner) error {

	seg, err := newSegment(s.Root, s.generateSegmentId())
	if err != nil {
		return err
	}

	for scanner.Scan() {
		chunk := scanner.ScanValue()
		seg.write(chunk)
	}
	seg.generateSnapshot()
	s.Segments = append(s.Segments, seg)
	return seg.sync()
}

// 倒序扫描segment文件，直到查询key
func (s *SSTable) Get(key string) ([]byte, error) {

	//布隆过滤器，确认key是否存在

	for i := len(s.Segments) - 1; i >= 0; i-- {
		chunk, err := s.Segments[i].get(key)
		if err != nil {
			return nil, err
		}
		if chunk != nil {
			if chunk.Deleted {
				return nil, nil
			}
			return chunk.Value, nil
		}
	}
	return nil, nil
}

func (s *SSTable) Close() {

	for i := range s.Segments {
		s.Segments[i].close()
	}
}
