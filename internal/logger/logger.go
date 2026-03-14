package logger

import (
	"sync"

	"go.uber.org/zap"
)

var (
	log  *zap.Logger
	once sync.Once
)

func Init(isDev bool) {
	once.Do(func() {
		var err error
		if isDev {
			log, err = zap.NewDevelopment()
		} else {
			log, err = zap.NewProduction()
		}
		if err != nil {
			panic(err)
		}
	})
}

func L() *zap.Logger {
	if log == nil {
		Init(true)
	}
	return log
}
