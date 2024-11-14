package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Jasonbourne723/platodb/internal/database"
)

func main() {

	db, err := database.NewDB()
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()

	for i := 3000000; i < 4000000; i++ {
		index := strconv.Itoa(i)
		db.Set("key:"+index, []byte("value"+index))
	}

	elapsed := time.Since(start)
	fmt.Printf("写入耗时 elapsed: %v\n", elapsed)

	for i := 0; i < 10000; i++ {
		print(db, i)
	}
	var a int
	fmt.Scanln(&a)

}

func print(db *database.DB, i int) {
	key := "key:" + strconv.Itoa(i)
	if val, err := db.Get(key); err == nil {
		fmt.Printf("key: %v,val: %v\n", key, string(val))
	} else {
		fmt.Println(err)
	}
}
