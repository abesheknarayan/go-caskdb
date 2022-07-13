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
	// utils.Logger.SetLevel(logrus.ErrorLevel)

	booksDb, err := store.InitDb("test1")
	if err != nil {
		log.Fatalf("Failed to initialize DB %v", err)
	}

	// utils.Logger.SetLevel(logrus.ErrorLevel)

	N := 10000
	M := 700
	m := make(map[string]string)
	var allKeys []string
	for i := 0; i < N; i++ {
		x := rand.Int() % 2
		switch x {
		case 0:
			{
				key := fmt.Sprintf("Key: %d", (rand.Int()%M + 1))
				value := fmt.Sprintf("Value: %d", (rand.Int()%M + 1))
				booksDb.Put(key, value)
				allKeys = append(allKeys, key)
				m[key] = value
			}
		case 1:
			{
				nKey := allKeys[rand.Intn(len(allKeys))]
				val, exists := m[nKey]
				if !exists {
					continue
				}
				nval := booksDb.Get(nKey)
				if nval != val {
					utils.Logger.Errorf("Values are not equal for key: %s, expected: %s, got %s", nKey, val, nval)
				}
			}
		}
	}

	// for i := 0; i < 1000; i++ {
	// 	// key := utils.GetRandomString(rand.Int()%10 + 1)
	// 	// value := utils.GetRandomString(rand.Int()%10 + 1)
	// 	key := fmt.Sprintf("Key %d", rand.Int()%3000)
	// 	value := fmt.Sprintf("Value %d", rand.Int()%3000)
	// 	// fmt.Println(key, value)
	// 	booksDb.Put(key, value)
	// }

	// utils.Logger.Debugln(booksDb.Get("Key 123"))
	// utils.Logger.Debugln(booksDb.Get("Key 33"))

	// utils.Logger.Debugln(booksDb.Get("Key 477"))
	// utils.Logger.Debugln(booksDb.Get("Key 1"))
	// utils.Logger.Debugln(booksDb.Get("Key 930"))

	booksDb.CloseDB()
	// booksDb.Cleanup()
}
