package mpsrc

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	// "github.com/prometheus/client_golang/prometheus"
)

type AdminHttpServer struct {
	ginServer *gin.Engine
}

// 输出版本号信息
func handleVersion(c *gin.Context) {
	c.String(http.StatusOK, "Meitudns-"+VERSION)
}

func (ads *AdminHttpServer) setupRouters() {
	engine := ads.ginServer
	// prometheus 统计
	// engine.GET("/metrics", gin.WrapH(prometheus.Handler()))
	// 输出版本信息
	engine.GET("/version", handleVersion)
}

// 后台功能的 http 服务应该只跑在内网的网卡
func (ads *AdminHttpServer) StartHttp(host string, port int, needAccessLog bool, logDir string) {
	ads.ginServer = GetDefaultGinEngine(needAccessLog, "amdin", logDir)
	ads.setupRouters()
	mpLogger.Info("start admin server successfully.")
	fmt.Println("start admin server successfully.")
	err := ads.ginServer.Run(host + ":" + strconv.Itoa(port))
	if err != nil {
		fmt.Printf("start admin server failed, %s\n", err)
		os.Exit(1)
	}
}
