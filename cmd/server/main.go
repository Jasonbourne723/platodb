package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/Jasonbourne723/platodb/internal/database"
)

func main() {

	db, err := database.NewDB()
	if err != nil {
		log.Fatal(err)
	}

	db.Set("key:1", []byte("value1"))
	db.Set("key:2", []byte("value2"))
	db.Set("key:3", []byte("value3"))

	for _, v := range [...]int{1, 2, 3} {
		print(db, v)
	}

	db.Del("key:2")

	fmt.Printf("\"-----------\": %v\n", "-----------")

	for _, v := range [...]int{1, 2, 3} {
		print(db, v)
	}

	db.Set("key:2", []byte("value5"))
	db.Set("key:1", []byte("value6"))

	fmt.Printf("\"-----------\": %v\n", "-----------")

	for _, v := range [...]int{1, 2, 3} {
		print(db, v)
	}

}

func print(db *database.DB, i int) {
	key := "key:" + strconv.Itoa(i)
	if val, err := db.Get(key); err == nil {
		fmt.Printf("val: %v\n", string(val))
	}
}
