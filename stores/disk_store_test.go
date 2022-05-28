package stores

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *DiskStore
var tempDir string // for storing db

func TestGet(t *testing.T) {
	value := "pro tester"
	db.Set("name", value)
	assert.Equal(t, value, db.Get("name"), "Values are not equal!!")
}

func TestInvalidKey(t *testing.T) {
	// subject to change in future
	assert.Equal(t, "", db.Get("random_key"))
}

func TestPersistance(t *testing.T) {
	db.Set("football", "cr7")
	db.CloseDB()
	var err error
	db, err = InitDb("testdb", tempDir)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, "cr7", db.Get("football"), "Persistance failure!")
}

func TestDbCleaup(t *testing.T) {
	db.Set("name", "God")
	db.CloseDB()
	db.Cleanup()
	assert.Equal(t, "", db.Get("name"), "Expected empty value")
}

func setupTests(t *testing.T) {
	fmt.Println("running setup")
	tempDir = t.TempDir()
	fmt.Println(tempDir)
	var err error
	db, err = InitDb("testdb", tempDir)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func cleanupTests() {
	log.Println("Cleaning up tests")
	db.CloseDB()
	db.Cleanup()
}

// all setup is done here as this runs first, call all tests from here
func TestMain(m *testing.M) {
	setupTests(&testing.T{})
	exit := m.Run()
	cleanupTests()
	os.Exit(exit)
}
