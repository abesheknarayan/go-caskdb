package utils

import (
	"io"
	"log"
	"os"

	"github.com/abesheknarayan/go-caskdb/pkg/config"
	"github.com/sirupsen/logrus"
)

// instance
var Logger *logrus.Logger

func InitLogger() {
	var (
		filename = "./logs/dblogs.log"
		loglevel = logrus.DebugLevel
	)
	Logger = &logrus.Logger{
		Formatter: &logrus.JSONFormatter{},
	}
	Logger.SetLevel(loglevel)
	Logger.SetFormatter(&logrus.TextFormatter{
		ForceColors: true,
	})

	// get env and choose accordingly

	var writer io.Writer

	var f *os.File

	if config.Config.Stage != "Test" {
		var err error
		f, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	switch config.Config.Stage {
	case "Dev":
		{
			writer = io.MultiWriter(f, os.Stdout)
		}
	case "Test":
		{
			writer = os.Stdout
			Logger.Level = logrus.ErrorLevel
		}
	case "Prod":
		{
			writer = f
		}
	}

	Logger.SetOutput(writer)

}
