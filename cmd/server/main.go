package main

import (
	"fmt"
	"log"

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
	db.Set("key:4", []byte("value4"))
	db.Set("key:5", []byte("value5"))
	db.Set("key:6", []byte("value6"))
	db.Set("key:7", []byte("value7"))

	val6, _ := db.Get("key:6")
	fmt.Printf("val6: %v\n", string(val6))

	val8, _ := db.Get("key:8")
	fmt.Printf("val8: %v\n", string(val8))

	val4, _ := db.Get("key:4")
	fmt.Printf("val4: %v\n", string(val4))

}
