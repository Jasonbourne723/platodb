package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/Jasonbourne723/platodb/internal/database"
)

func main() {

	db, err := database.NewDB(database.Dir("data", "data/wal"), database.SegmentSize(int32(100)))
	if err != nil {
		log.Fatal(err)
	}
	// for i := 0; i < 100000; i++ {
	// 	db.Set("key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))
	// }

	for i := 0; i < 100000; i++ {
		key := "key" + strconv.Itoa(i)
		if c, err := db.Get(key); err == nil {
			fmt.Printf("key:%s,value:%s\n", key, string(c))
		}
	}

	db.Shutdown()
}
