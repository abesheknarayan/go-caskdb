package utils

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

// instance
var Logger *logrus.Logger

func InitLogger() {
	var (
		filename = "./logs/dblogs.log"
		loglevel = logrus.DebugLevel
	)
	logrus.SetLevel(loglevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		ForceColors: true,
	})
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		logrus.Fatalf(err.Error())
	}
	mw := io.MultiWriter(f, os.Stdout)
	logrus.SetOutput(mw)
}
