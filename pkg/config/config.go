package config

import (
	"os"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type ConfigStruct struct {
	Path string
}

var Config *ConfigStruct

func LoadConfigFromEnv() {

	var l = log.WithFields(logrus.Fields{
		"method": "LoadConfigFromEnv",
	})
	err := godotenv.Load(".env")

	if err != nil {
		l.Fatalf("Failed to load env %v", err)
	}

	path := os.Getenv("DB_PATH")

	Config = &ConfigStruct{
		Path: path,
	}
}
