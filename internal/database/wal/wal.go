package database

import (
	"os"
	"path"
	"time"

	"github.com/Jasonbourne723/platodb/internal/database/common"
)

const (
	ROOT   = "D://platodb//wal/"
	SUFFIX = ".log"
)

func NewWal() (*Wal, error) {

	filePath := path.Join(ROOT, "wal", time.Now().Format("20060102150405"), SUFFIX)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	wal := &Wal{
		file:     file,
		filePath: filePath,
		utils:    common.NewUtils(),
	}
	return wal, nil
}

type Wal struct {
	file     *os.File
	filePath string
	utils    *common.Utils
}

func (w *Wal) Write(chunk *common.Chunk) error {

	bytes, err := w.utils.Encode(chunk)
	if err != nil {
		return err
	}
	_, err = w.file.Write(bytes)
	if err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *Wal) Delete() {
	w.file.Close()
	os.Remove(w.filePath)
}
