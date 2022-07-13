package disk_store

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

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
func Test_MultipleSegments(t *testing.T) {
	N := 1000
	m := make(map[string]string)
	allKeys := make([]string, N)
	for i := 0; i < N; i++ {
		key := utils.GetRandomString(rand.Int()%10 + 1)
		value := utils.GetRandomString(rand.Int()%10 + 1)
		allKeys = append(allKeys, key)
		m[key] = value
		db.Put(key, value)
	}

	numChecks := rand.Intn(N-1) + 1

	for i := 0; i < numChecks; i++ {
		nKey := allKeys[rand.Intn(N)]
		assert.Equal(t, m[nKey], db.Get(nKey), "Values are not equal!!")
	}
}

func Test_MergeCompaction(t *testing.T) {
	N := 5000
	m := make(map[string]string)
	allKeys := make([]string, N)
	for i := 0; i < N; i++ {
		// maintaining a field of just 300 elements
		key := fmt.Sprintf("Key: %d", (rand.Int()%300 + 1))
		value := fmt.Sprintf("Value: %d", (rand.Int()%300 + 1))
		allKeys = append(allKeys, key)
		m[key] = value
		db.Put(key, value)
	}

	numChecks := rand.Intn(N-1) + 1

	for i := 0; i < numChecks; i++ {
		nKey := allKeys[rand.Intn(N)]
		assert.Equal(t, m[nKey], db.Get(nKey), "Values are not equal!!")
	}
}

func BenchmarkInsertionAlone10000(b *testing.B) {
	N := 10000
	M := 700
	for i := 0; i < N; i++ {
		// maintaining a field of just 300 elements
		key := fmt.Sprintf("Key: %d", (rand.Int()%M + 1))
		value := fmt.Sprintf("Value: %d", (rand.Int()%M + 1))
		db.Put(key, value)
	}

}

func BenchmarkInsertionWithGet10000(b *testing.B) {
	N := 10000
	M := 700
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
