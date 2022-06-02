package utils

import (
	"os"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// instance
var Logger *logrus.Logger

func InitLogger() {
	var (
		filename = "./logs/dblogs.log"
		loglevel = log.DebugLevel
	)
	log.SetLevel(loglevel)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf(err.Error())
	}
	log.SetOutput(f)
}
