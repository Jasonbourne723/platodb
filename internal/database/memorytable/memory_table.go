package memorytable

type Memorytable interface {
	Set(key string, value []byte)
	Get(key string) []byte
	Del(key string)
	Size() int64
	Scan() bool
	ScanValue() (key string, value []byte, deleted bool)
}
