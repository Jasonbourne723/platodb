package sstable

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
