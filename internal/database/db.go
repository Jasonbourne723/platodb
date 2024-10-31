package database

type Db struct {
}

func (db *Db) Get(key string) ([]byte, error) {
	return []byte(""), nil
}

func (db *Db) Set(key string, value []byte) error {
	return nil
}
