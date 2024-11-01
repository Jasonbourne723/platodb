package memorytable

type Memorytable interface {
	Set(key string, value []byte)
	Get(key string) []byte
}
