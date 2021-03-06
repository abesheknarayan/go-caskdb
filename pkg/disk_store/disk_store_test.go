package disk_store

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/abesheknarayan/go-caskdb/pkg/config"
	"github.com/abesheknarayan/go-caskdb/pkg/utils"
	"github.com/stretchr/testify/assert"
)

var db *DiskStore

var tempDir string // for storing db

func Test_Get(t *testing.T) {
	value := "pro tester"
	db.Put("name", value)
	assert.Equal(t, value, db.Get("name"), "Values are not equal!!")
}

func Test_InvalidKey(t *testing.T) {
	// subject to change in future
	assert.Equal(t, "", db.Get("random_key"))
}

func Test_Persistance(t *testing.T) {
	db.Put("football", "cr7")
	db.CloseDB()
	var err error
	db, err = InitDb("testdb")
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, "cr7", db.Get("football"), "Persistance failure!")
}

func InsertAndRead(N int, t *testing.T) {
	t_db, err := InitDb(fmt.Sprintf("normalDb%d", time.Now().Unix()))
	if err != nil {
		t.Fatal(err)
	}
	m := make(map[string]string)
	allKeys := make([]string, N)
	for i := 0; i < N; i++ {
		key := utils.GetRandomString(rand.Int()%10 + 3)
		value := utils.GetRandomString(rand.Int()%10 + 5)
		allKeys = append(allKeys, key)
		m[key] = value
		t_db.Put(key, value)
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
		nval := t_db.Get(nKey)
		assert.Equal(t, val, nval, "Values are not equal!!")
	}
}
func Test_InsertionFirstAndReads(t *testing.T) {
	N := 100000
	config.Config.MemtableSizeLimit = 4 * 1024 * 1024
	InsertAndRead(N, t)
	N = 10000
	config.Config.MemtableSizeLimit = 4 * 1024
	InsertAndRead(N, t)
}

func InsertWithConcurrentReads(N int, M int, t *testing.T) {
	m := make(map[string]string)
	var allKeys []string
	t_db, err := InitDb(fmt.Sprintf("concurrentDb%d", time.Now().Unix()))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		x := rand.Int() % 2
		switch x {
		case 0:
			{
				// maintaining a field of 700 elements
				key := fmt.Sprintf("Key: %d", (rand.Int()%M + 1))
				value := fmt.Sprintf("Value: %d", (rand.Int()%M + 1))
				m[key] = value
				allKeys = append(allKeys, key)
				t_db.Put(key, value)
			}
		case 1:
			{
				if len(allKeys) == 0 {
					continue
				}
				nKey := allKeys[rand.Intn(len(allKeys))]
				val, exists := m[nKey]
				if !exists {
					continue
				}
				nval := t_db.Get(nKey)
				assert.Equal(t, val, nval, "Values are not equal!!")
			}
		}
	}
}

func Test_InsertionWithConcurrentReads(t *testing.T) {
	N := 100000
	M := 3167
	config.Config.MemtableSizeLimit = 4 * 1024 * 1024
	InsertWithConcurrentReads(N, M, t)
	N = 10000
	M = 967
	config.Config.MemtableSizeLimit = 4 * 1024
	InsertWithConcurrentReads(N, M, t)
}

func BenchmarkInsertionAlone100000(b *testing.B) {
	N := 100000
	M := 3167
	config.Config.MemtableSizeLimit = 4 * 1024 * 1024
	for i := 0; i < N; i++ {
		// maintaining a field of just 300 elements
		key := fmt.Sprintf("Key: %d", (rand.Int()%M + 1))
		value := fmt.Sprintf("Value: %d", (rand.Int()%M + 1))
		db.Put(key, value)
	}

}

func BenchmarkInsertionWithGet100000(b *testing.B) {
	N := 100000
	M := 3167
	config.Config.MemtableSizeLimit = 4 * 1024 * 1024
	m := make(map[string]string)
	var allKeys []string

	for i := 0; i < N; i++ {
		x := rand.Int() % 2
		switch x {
		case 0:
			{
				// maintaining a field of 700 elements
				key := fmt.Sprintf("Key: %d", (rand.Int()%M + 1))
				value := fmt.Sprintf("Value: %d", (rand.Int()%M + 1))
				m[key] = value
				allKeys = append(allKeys, key)
				db.Put(key, value)
			}
		case 1:
			{
				if len(allKeys) == 0 {
					continue
				}
				nKey := allKeys[rand.Intn(len(allKeys))]
				val, exists := m[nKey]
				if !exists {
					continue
				}
				nval := db.Get(nKey)
				assert.Equal(b, val, nval, "Values are not equal!!")
			}
		}
	}
}

func Test_DbCleanup(t *testing.T) {
	db.Put("name", "God")
	db.CloseDB()
	db.Cleanup()
	assert.Equal(t, "", db.Get("name"), "Expected empty value")
}

// tests for many number of randomly generated keys so that many segment files are created and looked up

func setupTests(t *testing.T) {
	fmt.Println("running setup")
	tempDir = t.TempDir()
	fmt.Println(tempDir)
	config.LoadConfigFromEnv()
	config.Config.SetConfig(tempDir)
	utils.InitLogger()
	var err error
	db, err = InitDb("testdb")
	if err != nil {
		t.Fatalf(err.Error())
	}
}

// all setup is done here as this runs first, call all tests from here
func TestMain(m *testing.M) {
	setupTests(&testing.T{})
	exit := m.Run()
	os.Exit(exit)
}
