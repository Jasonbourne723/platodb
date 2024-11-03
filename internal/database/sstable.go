package database

import (
	"bufio"
	"os"
)

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB = 1 << (10 * iota)
	GB = 1 << (10 * iota)

	BLOCK_SIZE = 32 * KB
)

type SSTable struct {
	Segments []*Segment
}

type Segment struct {
	fd *os.File
}

func (seg *Segment) ReadBlock() ([]byte, error) {

	reader := bufio.NewReader(seg.fd)
	buf := make([]byte, BLOCK_SIZE)

	n, err := reader.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
