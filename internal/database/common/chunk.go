package common

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB = 1 << (10 * iota)
	GB = 1 << (10 * iota)
)

type Chunk struct {
	Key     string
	Value   []byte
	Deleted bool
}
