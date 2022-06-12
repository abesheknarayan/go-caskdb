package main

import (
	"fmt"
	"log"
	"math/rand"

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

	for i := 0; i < 10; i++ {
		key := utils.GetRandomString(rand.Int()%10 + 1)
		value := utils.GetRandomString(rand.Int()%10 + 1)
		fmt.Println(key, value)
		booksDb.Put(key, value)
	}

	booksDb.CloseDB()
	booksDb.Cleanup()
}
