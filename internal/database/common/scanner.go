package common

type Scanner interface {
	Scan() bool
	ScanValue() *Chunk
}
