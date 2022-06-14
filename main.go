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

	booksDb, err := store.InitDb("test1")
	if err != nil {
		log.Fatalf("Failed to initialize DB %v", err)
	}

	for i := 0; i < 1000; i++ {
		// key := utils.GetRandomString(rand.Int()%10 + 1)
		// value := utils.GetRandomString(rand.Int()%10 + 1)
		key := fmt.Sprintf("Key %d", i+1)
		value := fmt.Sprintf("Value %d", i+1)
		fmt.Println(key, value)
		booksDb.Put(key, value)
	}

	utils.Logger.Debugln(booksDb.Get("Key 123"))
	utils.Logger.Debugln(booksDb.Get("Key 33"))

	utils.Logger.Debugln(booksDb.Get("Key 477"))
	utils.Logger.Debugln(booksDb.Get("Key 1"))
	utils.Logger.Debugln(booksDb.Get("Key 930"))

	booksDb.CloseDB()
	booksDb.Cleanup()
}
