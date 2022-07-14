package config

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type ConfigStruct struct {
	Stage             string // Dev || Prod || Test
	Path              string
	MemtableSizeLimit uint64
}

var Config *ConfigStruct

// for go tests
func (c *ConfigStruct) SetConfig(path string) {
	c.Path = path
}

func LoadConfigFromEnv() {

	stage, exists := os.LookupEnv("CASKDB_ENV")
	var path string
	if !exists {
		if flag.Lookup("test.v") != nil {
			// in testing
			stage = "Test"
		} else {
			stage = "Dev"
			err := godotenv.Load(".env")
			if err != nil {
				log.Fatalf("Failed to load env %v", err)
			}
			path, _ = os.LookupEnv("DB_PATH") // will be empty if DB_PATH is empty [In case of tests above func will be used to set path]
		}
	}
	Config = &ConfigStruct{
		Stage:             stage,
		Path:              path,
		MemtableSizeLimit: MAX_MEMTABLE_SIZE,
	}
	fmt.Println(Config)

}
