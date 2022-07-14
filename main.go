package main

import (
	"fmt"
	"log"
	"math/rand"

	config "github.com/abesheknarayan/go-caskdb/pkg/config"
	store "github.com/abesheknarayan/go-caskdb/pkg/disk_store"
	utils "github.com/abesheknarayan/go-caskdb/pkg/utils"
)

func TestInsertionAndRead(db *store.DiskStore) {
	N := 10000
	m := make(map[string]string)
	allKeys := make([]string, N)
	for i := 0; i < N; i++ {
		key := utils.GetRandomString(rand.Int()%10 + 3)
		value := utils.GetRandomString(rand.Int()%10 + 5)
		allKeys = append(allKeys, key)
		m[key] = value
		db.Put(key, value)
	}

	numChecks := rand.Intn(N-1) + 1

	for i := 0; i < numChecks; i++ {
		if len(allKeys) == 0 {
			continue
		}
		nKey := allKeys[rand.Intn(len(allKeys))]
		val, exists := m[nKey]
		if !exists {
			continue
		}
		nval := db.Get(nKey)
		if val != nval {
			utils.Logger.Errorf("Values are not equal for key: %s, expected: %s, got %s", nKey, val, nval)
		}
	}
}

func TestConcurrentInsertionAndRead(db *store.DiskStore) {
	N := 10000
	M := 1400
	m := make(map[string]string)
	var allKeys []string
	for i := 0; i < N; i++ {
		x := rand.Int() % 2
		switch x {
		case 0:
			{
				key := fmt.Sprintf("Key: %d", (rand.Int()%M + 1))
				value := fmt.Sprintf("Value: %d", (rand.Int()%M + 1))
				db.Put(key, value)
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
				nval := db.Get(nKey)
				if nval != val {
					utils.Logger.Errorf("Values are not equal for key: %s, expected: %s, got %s", nKey, val, nval)
				}
			}
		}
	}
}

func main() {

	config.LoadConfigFromEnv()
	utils.InitLogger()
	// utils.Logger.SetLevel(logrus.ErrorLevel)

	booksDb, err := store.InitDb("test1")
	if err != nil {
		log.Fatalf("Failed to initialize DB %v", err)
	}

	TestInsertionAndRead(booksDb)
	// TestConcurrentInsertionAndRead(booksDb)

	booksDb.CloseDB()
	// booksDb.Cleanup()
}
