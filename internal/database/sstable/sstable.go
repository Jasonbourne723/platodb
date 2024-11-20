package sstable

import (
	"os"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

type SSTable struct {
	Segments []*segment
	Root     string
}

// NewSSTable initializes a new SSTable instance with the given root directory.
// It loads existing segments from the root directory and appends them to the SSTable.
// Returns a pointer to the SSTable and an error if any occurs during initialization or loading.
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

// load reads all segment files from the SSTable's root directory and adds them to the SSTable's Segments slice.
// It ensures the root directory exists before attempting to read files.
// Any error encountered during file reading or segment loading is returned.
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

// generateSegmentId returns the next segment ID for the SSTable by incrementing the ID of the last segment in the Segments slice.
// If the slice is empty, it returns 1.
func (s *SSTable) generateSegmentId() int64 {
	if len(s.Segments) == 0 {
		return 1
	}
	return s.Segments[len(s.Segments)-1].id + 1
}

// Write reads data from the provided Scanner and writes it into a new segment within the SSTable.
// It creates a new segment, iterates over the Scanner, writes each chunk, generates a snapshot for the segment,
// appends the segment to the SSTable's segments, and syncs the segment to disk.
// Returns an error if any occurs during segment creation, writing, or syncing.
func (s *SSTable) Write(scanner common.Scanner) error {

	seg, err := newSegment(s.Root, s.generateSegmentId())
	if err != nil {
		return err
	}

	for scanner.Scan() {
		chunk := scanner.ScanValue()
		err := seg.write(chunk)
		if err != nil {
			return err
		}
	}
	err = seg.generateSnapshot()
	if err != nil {
		return err
	}
	s.Segments = append(s.Segments, seg)
	return seg.sync()
}

// Get retrieves the value associated with the given key from the SSTable.
// It iterates through the segments in reverse order to find the latest value for the key.
// If the key is found and not marked as deleted, it returns the corresponding value; otherwise, it returns nil.
// If an error occurs during the retrieval process, it is returned along with the nil value.
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

// Close shuts down all the segments in the SSTable by calling the close method on each one.
func (s *SSTable) Close() {

	for i := range s.Segments {
		s.Segments[i].close()
	}
}
