package mpsrc

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strconv"
)

type HttpServer struct {
	ginServer *gin.Engine
}

const (
	INVAILD_ARGUMENT_CODE = 10000 + iota // 参数不合法
	INVAILD_INNER_CODE                   // 内部错误
	INVAILD_RESULT_CODE                  // 结果错误
)

var (
	errorMsgMap map[int]string
)

func initErrorMsgMap() {
	errorMsgMap = make(map[int]string)
	errorMsgMap[INVAILD_ARGUMENT_CODE] = "invaild argument error"
	errorMsgMap[INVAILD_INNER_CODE] = "inner error"
	errorMsgMap[INVAILD_RESULT_CODE] = "no result"
}

/*
* 根据 code 输出错误信息
 */
func echoErrorMsg(c *gin.Context, code int) bool {
	if code < 0 {
		return false
	}

	msg := "unknown"
	if errMsg, ok := errorMsgMap[code]; ok {
		msg = errMsg
	}
	c.JSON(http.StatusOK, gin.H{
		"err_code": code,
		"err_msg":  msg,
	})
	return true
}

/*
* 获取个人动态，根据时间段获取
*/
func handleGetPersonalTimeline(c *gin.Context) {
	timeBegin := c.Query("timebegin")
	timeEnd := c.Query("timeend")
	userID := c.Query("userid")
	if userID == "" || timeBegin == "" || timeEnd == "" {
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
		return
	}
	data, err := getPersonalTimeline(timeBegin, timeEnd, userID)
	if err != nil {
		echoErrorMsg(c, INVAILD_INNER_CODE)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

/*
* 添加个人动态，主动向前不超过阀值的粉丝（按照时间升序排序）推送动态
*/
func handleAddPersonalTimeline(c *gin.Context) {
	userID := c.PostForm("userid")
	timestamp := c.PostForm("timestamp")
	value := c.PostForm("value")
	if userID == "" || timestamp == "" || value == "" {
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
		return
	}
	//md5处理，生成ValueKey
	valueKey := StoreValue(value, userID)
	addPersonalTimeline(userID, timestamp, valueKey)
	c.JSON(http.StatusOK, gin.H{"data": true})
}

/*
* 删除个人动态
*/
func handleDelPersonalTimeline(c *gin.Context) {
	userID := c.PostForm("userid")
	timestamp := c.PostForm("timestamp")
	value := c.PostForm("value")
	if userID == "" || timestamp == "" || value == "" {
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
		return
	}

	delPersonalTimeline(userID, timestamp, value)
	c.JSON(http.StatusOK, gin.H{"data": true})
}

/*
* 将请求放入kafka队列
*/
func handlePostPersonalTimeline(c *gin.Context) {
	action := c.PostForm("action")
	switch action {
	case "add":
		handleAddPersonalTimeline(c)
	case "delete":
		handleDelPersonalTimeline(c)
	default:
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
	}
}

/*
* 获取好友动态，将所有未push的likes对象的动态拉过来并综合结果排序
*/
func handleGetFriendsTimeline(c *gin.Context) {
	timeBegin := c.Query("timebegin")
	timeEnd := c.Query("timeend")
	userID := c.Query("userid")
	if userID == "" || timeBegin == "" || timeEnd == "" {
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
		return
	}

	data, err := getFriendsTimeline(timeBegin, timeEnd, userID)
	if err != nil {
		echoErrorMsg(c, INVAILD_INNER_CODE)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

/*
* 获取好友信息，包括粉丝（数）和关注（数）
*/
func handleGetFriendsInfo(c *gin.Context) {
	userID := c.Query("userid")
	if userID == "" {
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
		return
	}

	likes := getFriendsInfo(userID, LIKES)
	fans := getFriendsInfo(userID, FANS)
	c.JSON(http.StatusOK, gin.H{"fans": fans,
		"likes": likes})
}

/*
* 增加或删除好友关系，删除好友时需要做一些附加的清理操作，包括但不限于
* 清理pushtimeline的内容（如果有的话），如果因为该删除操作引起对方
* push列表变化，则需要通知对方推送部分历史消息到新被push的对象的pushtimeline;
* 同理，添加操作也需要部分过渡性操作。
*/
func handlePostFriendsInfo(c *gin.Context) {
	var info, infoType string
	action := c.PostForm("action")
	like := c.PostForm("like")
	fan := c.PostForm("fan")
	userID := c.PostForm("userid")
	if (like == "" && fan == "") || userID == "" {
		echoErrorMsg(c, INVAILD_ARGUMENT_CODE)
		return
	}
	if like == "" {
		info = fan
		infoType = FANS
	} else {
		info = like
		infoType = LIKES
	}
	err := updateFriendsInfo(info, userID, infoType, action)
	if err != nil {
		echoErrorMsg(c, INVAILD_INNER_CODE)
	} else {
		c.JSON(http.StatusOK, gin.H{"data": true})
	}
}
//ok
func handleUnreadNum(c *gin.Context) {
	//配合redis的watch
	userID := c.Query("userid")
	conn := redisPool.GetClient(true)
	if conn == nil || userID == "" {
		echoErrorMsg(c, INVAILD_INNER_CODE)
		return
	}
	defer conn.Close()
	key := userID + UNREAD
	unRead, err := redis.Uint64(conn.Do("GET", key))
	if err != nil {
		echoErrorMsg(c, INVAILD_RESULT_CODE)
		return
	}
	c.JSON(http.StatusOK, gin.H{"unread": unRead})
	conn.Do("SET", key, 0)
}

/*
* http 服务的所有路由处理在这里做
 */
func (hs *HttpServer) setupRouters() {
	engine := hs.ginServer
	//查找
	engine.GET("api/personaltimeline", handleGetPersonalTimeline)
	engine.GET("api/friendstimeline", handleGetFriendsTimeline)
	engine.GET("api/friendsinfo", handleGetFriendsInfo)
	engine.GET("api/unreadnum", handleUnreadNum)
	//增加和删除
	engine.POST("api/personaltimeline", handlePostPersonalTimeline)
	engine.POST("api/friendsinfo", handlePostFriendsInfo)
}

/*
* http 服务启动函数
* @param host: 指定启动的ip
* @param port: 端口
* @param logDir: 日志存放路径
 */
func (hs *HttpServer) StartHttp(host string, port int,
 needAccessLog bool, logDir string) {
	initErrorMsgMap()
	go runProducer()
	go watchDataChange()
	go updatePushFriendsTimelineOfDB()
	go updateLikesOfDB()
	go updateFansOfDB()
	go updatePersonalTimelineOfDB()
	go updateValueOfDB()
	hs.ginServer = GetDefaultGinEngine(needAccessLog, "http", logDir)
	hs.setupRouters()
	mpLogger.Info("start http server successfully.")
	fmt.Println("start http server successfully.")
	err := hs.ginServer.Run(host + ":" + strconv.Itoa(port))
	if err != nil {
		fmt.Printf("start http server failed, %s\n", err)
		os.Exit(1)
	}
}
