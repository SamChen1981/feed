package mpsrc

import (
	"encoding/json"
	"feed/storage"
	"github.com/gin-gonic/gin"
	"gitlab.meitu.com/platform/gocommons/httpsrv"
)

const (
	ERRORTTL int64 = 10
)

var (
	ErrorValue, _ = json.Marshal("get the key errors")
	ErrorResult   = &storage.Item{
		Value:    ErrorValue,
		ExpireAt: ERRORTTL,
	}
)

func GetDefaultGinEngine(needAccessLog bool, args ...string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	if !needAccessLog {
		// 不打开 access log
		engine.Use(gin.Recovery())
		return engine
	}

	name := "default"
	logDir := "./logs"
	if args != nil && len(args) > 0 {
		name = args[0]
	}
	if args != nil && len(args) > 1 {
		logDir = args[1]
	}
	accessLogger := httpsrv.NewAccessLogger(httpsrv.AccessLogOptions{
		Rotatable:      true,
		Pattern:        httpsrv.DefaultLogPattern,
		Prefix:         name + ".access",
		Suffix:         ".log",
		FileDateFormat: ".20060102",
		Directory:      logDir,
	})
	engine.Use(gin.Recovery(), httpsrv.AccessLoggerFunc(accessLogger))

	return engine
}

func hash(id uint64) uint64 {
	if id % 2 == 0 {
		return 0
	} 
	return 1
}