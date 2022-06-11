package main

import (
	"fmt"
	"log"

	config "github.com/abesheknarayan/go-caskdb/pkg/config"
	store "github.com/abesheknarayan/go-caskdb/pkg/disk_store"
	utils "github.com/abesheknarayan/go-caskdb/pkg/utils"
)

func main() {

	config.LoadConfigFromEnv()
	utils.InitLogger()

	booksDb, err := store.InitDb("test")
	if err != nil {
		log.Fatalf("Failed to initialize DB %v", err)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key %d", i)
		value := fmt.Sprintf("val %d", i)
		booksDb.Put(key, value)
	}

	fmt.Println(booksDb.Get("key 678"))
	fmt.Println(booksDb.Get("key 12"))

	fmt.Println(booksDb.Get("key 999"))

	booksDb.CloseDB()
	booksDb.Cleanup()
}
