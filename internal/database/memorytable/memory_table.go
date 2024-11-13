package memorytable

import "github.com/Jasonbourne723/platodb/internal/database/common"

type Memorytable interface {
	Set(key string, value []byte, deleted bool)
	Get(key string) []byte
	Size() int64
	common.Scanner
}
