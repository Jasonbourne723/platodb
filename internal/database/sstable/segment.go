package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

const (
	BlockSize    = 64 * common.KB
	FileModePerm = 0644
	SegSuffix    = ".seg"
	SpSuffix     = ".sp"
)

type snapshotBlock struct {
	min string
	max string
}

type segment struct {
	id        int64
	file      *os.File
	filePath  string
	closed    int32
	blocks    []block
	snapshots []snapshotBlock
	size      int64
	utils     *common.Utils
}

// newSegment creates a new segment with the specified root directory and ID.
// It opens a file for the segment, initializes block and snapshot slices, and returns a pointer to the segment.
// If there's an error opening the file, it returns an error.
// Parameters:
//   - root: The directory where the segment file will be stored.
//   - id: The unique identifier for the segment.
//
// Returns:
//   - Pointer to the created segment, or nil and an error if creation fails.
func newSegment(root string, id int64) (*segment, error) {

	name := fmt.Sprintf("%06d%s", id, SegSuffix)
	filePath := path.Join(root, name)

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, FileModePerm)
	if err != nil {
		return nil, fmt.Errorf("segment文件打开失败:%w", err)
	}
	return &segment{
		id:        id,
		file:      file,
		filePath:  filePath,
		blocks:    make([]block, 0, 50),
		snapshots: make([]snapshotBlock, 0, 50),
		utils:     common.NewUtils(),
	}, nil
}

// loadSegment loads a segment from the given root directory and segment name.
// It validates the file format, extracts segment ID, opens the file, and initializes blocks.
// Returns a pointer to the segment and an error if any occurs during loading.
func loadSegment(root string, name string) (*segment, error) {
	if !strings.HasSuffix(name, SegSuffix) {
		return nil, errors.New("FILE FORMAT ERROR")
	}
	id, err := strconv.Atoi(strings.Split(name, ".")[0])
	if err != nil {
		return nil, err
	}
	filePath := path.Join(root, name)

	// 获取文件信息
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filePath, os.O_RDONLY, FileModePerm)
	if err != nil {
		return nil, err
	}
	seg := &segment{
		id:       int64(id),
		filePath: filePath,
		file:     file,
		closed:   0,
		size:     fileInfo.Size(),
	}
	err = seg.initBlocks()
	if err != nil {
		return nil, err
	}

	return seg, nil
}

// getSnapshotFilePath returns the file path for the snapshot file associated with the segment.
// It constructs the path by removing the Segment suffix from the filePath and appending the Snapshot suffix.
func (s *segment) getSnapshotFilePath() string {
	return strings.TrimSuffix(s.filePath, SegSuffix) + SpSuffix
}

// write encodes the provided chunk and adds it to the latest suitable block within the segment.
// It returns an error if the encoded chunk exceeds the block size or if there's an issue with block retrieval or data addition.
func (s *segment) write(chunk *common.Chunk) error {
	data, err := s.utils.Encode(chunk)
	if err != nil {
		return err
	}

	l := int64(len(data))

	if l > BlockSize {
		return errors.New("too large")
	}

	block, err := s.getLatestEnonghBlock(l)
	if err != nil {
		return err
	}
	return block.addChunk(chunk, data)
}

// get searches for a chunk with the specified key within the segment's blocks.
// It uses a binary search on the snapshots to find the potential block containing the key.
// If the key is found, the corresponding chunk and nil error are returned.
// If the key is not found or an error occurs, nil and the respective error are returned.
// If there are no snapshots, it implies no data, hence immediately returns nil, nil.
func (s *segment) get(key string) (chunk *common.Chunk, err error) {

	if len(s.snapshots) == 0 {
		return nil, nil
	}

	pos, ok := s.middleSearch(key, 0, int64(len(s.snapshots)-1))
	if ok {
		chunk, err := s.blocks[pos].get(key)
		if err != nil || chunk != nil {
			return chunk, err
		}
	}
	return nil, nil

	// for i := range s.blocks {
	// 	chunk, err := s.blocks[i].get(key)
	// 	if err != nil || chunk != nil {
	// 		return chunk, err
	// 	}
	// }
	// return nil, nil
}

// middleSearch performs a binary search within the segment to find the position of a given key.
// It returns the position and a boolean indicating whether the key was found.
// If the key is not found, position returned is -1.
// The search is conducted recursively between posBegin and posEnd indices of the snapshots slice.
func (s *segment) middleSearch(key string, posBegin int64, posEnd int64) (pos int64, ok bool) {

	if key < s.snapshots[posBegin].min || key > s.snapshots[posEnd].max {
		return -1, false
	}
	if key >= s.snapshots[posBegin].min && key <= s.snapshots[posBegin].max {
		return posBegin, true
	}
	if key >= s.snapshots[posEnd].min && key <= s.snapshots[posEnd].max {
		return posEnd, true
	}
	if posEnd == posBegin || posEnd == posBegin+1 {
		return -1, false
	}
	posMiddle := (posBegin + posEnd) / 2
	if key >= s.snapshots[posMiddle].min && key <= s.snapshots[posMiddle].max {
		return posMiddle, true
	}
	if key < s.snapshots[posMiddle].min {
		return s.middleSearch(key, posBegin, posMiddle)
	}
	return s.middleSearch(key, posMiddle, posEnd)
}

// loadSnapshot reads snapshot data from a file and populates the segment's snapshots slice with the parsed key ranges.
// It opens the snapshot file in read-only mode, iterates through the file to decode and construct snapshotBlock structs,
// and appends them to the segment's snapshots. The process stops when the end of the file is reached.
// An error is returned if there are issues reading the file.
func (s *segment) loadSnapshot() error {

	spFilePath := s.getSnapshotFilePath()
	f, err := os.OpenFile(spFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	for {
		minKeyLen := make([]byte, 4)
		if _, err := io.ReadFull(f, minKeyLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		minKeyLenUint32 := binary.BigEndian.Uint32(minKeyLen)
		minKeyBuf := make([]byte, minKeyLenUint32)
		if _, err := io.ReadFull(f, minKeyBuf); err != nil {
			return err
		}
		minKey := string(minKeyBuf)

		maxKeyLen := make([]byte, 4)
		if _, err := io.ReadFull(f, maxKeyLen); err != nil {
			return err
		}
		maxKeyLenUint32 := binary.BigEndian.Uint32(maxKeyLen)
		maxKeyBuf := make([]byte, maxKeyLenUint32)
		if _, err := io.ReadFull(f, maxKeyBuf); err != nil {
			return err
		}
		maxKey := string(maxKeyBuf)

		s.snapshots = append(s.snapshots, snapshotBlock{
			minKey, maxKey,
		})
	}
	return nil
}

// generateSnapshot creates a snapshot file for the segment by serializing the minimum and maximum keys of each block.
// It opens the snapshot file for writing, encodes the key length and keys into a buffer, writes the buffer content to the file,
// and updates the segment's snapshots slice with the block boundaries. The method ensures the file is synced to disk before returning.
// Returns an error if any I/O operation fails during the process.
func (s *segment) generateSnapshot() error {

	spFilePath := s.getSnapshotFilePath()
	f, err := os.OpenFile(spFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := &bytes.Buffer{}
	for i := range s.blocks {
		chunks := s.blocks[i].chunks
		firstChunk := chunks[0]
		minKeyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(minKeyLen, uint32(len(firstChunk.Key)))
		buf.Write(minKeyLen)
		buf.Write([]byte(firstChunk.Key))

		lastChunk := chunks[len(chunks)-1]
		maxKeyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(maxKeyLen, uint32(len(lastChunk.Key)))
		buf.Write(maxKeyLen)
		buf.Write([]byte(lastChunk.Key))
		s.snapshots = append(s.snapshots, snapshotBlock{
			min: firstChunk.Key,
			max: lastChunk.Key,
		})
	}

	_, err = f.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return f.Sync()
}

// initBlocks initializes the blocks for the segment based on loaded snapshots.
// It first loads the snapshot data, then creates a slice of blocks accordingly.
// Each block is associated with a segment and has a starting offset.
func (s *segment) initBlocks() error {

	if err := s.loadSnapshot(); err != nil {
		return err
	}

	//blockCount := int(math.Ceil(float64(s.size) / float64(BLOCK_SIZE)))
	s.blocks = make([]block, 0, len(s.snapshots))

	for i := 0; i < len(s.snapshots); i++ {
		s.blocks = append(s.blocks, newBlock(s, int64(i*BlockSize)))
	}
	return nil
}

// sync ensures all data written to the segment's file is flushed to the underlying storage.
// It calls the `Sync` method of the file handle associated with the segment.
// Returns an error if the synchronization fails.
func (s *segment) sync() error {
	return s.file.Sync()
}

// 关闭文件流
func (s *segment) close() error {
	if atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return s.file.Close()
	}
	return nil
}

// getLatestEnoughBlock returns the latest block in the segment that can accommodate a chunk of size l.
// If no such block exists, a new block is appended to the segment.
// The block is guaranteed to have enough space for the chunk.
// It adjusts the block size by writing empty bytes if necessary.
func (s *segment) getLatestEnonghBlock(l int64) (*block, error) {
	length := len(s.blocks)
	if length == 0 {
		s.blocks = append(s.blocks, newBlock(s, 0))
	} else if !s.blocks[length-1].enough(l) {
		_, err := s.file.Write(make([]byte, BlockSize-s.blocks[length-1].size))
		if err != nil {
			return nil, err
		}
		s.blocks = append(s.blocks, newBlock(s, int64(length*BlockSize)))
	}
	return &s.blocks[len(s.blocks)-1], nil
}
